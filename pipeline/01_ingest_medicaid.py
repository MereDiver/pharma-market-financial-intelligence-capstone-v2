"""Ingest real CMS Medicaid data into an idempotent Bronze Delta table."""

from __future__ import annotations

import sys
from pathlib import Path

# Databricks serverless executes Python files through ``exec`` and does not
# always populate ``__file__``. Its wrapper does expose the source as
# ``filename``, while normal Python execution continues to use ``__file__``.
SOURCE_PATH = globals().get("__file__") or globals().get("filename")
if not SOURCE_PATH:
    raise RuntimeError("Unable to determine the pipeline source path.")
ROOT = Path(str(SOURCE_PATH)).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pyspark.sql import SparkSession, functions as F, types as T

from config.job_args import configure_from_args
from config.project_config import CMS_DATASETS, load_config
from pipeline.cms_medicaid_client import CMSMedicaidClient, normalize_header

RAW_FIELDS = (
    "utilization_type", "state", "ndc", "labeler_code", "product_code", "package_size",
    "year", "quarter", "suppression_used", "product_name", "units_reimbursed",
    "number_of_prescriptions", "total_amount_reimbursed", "medicaid_amount_reimbursed",
    "non_medicaid_amount_reimbursed",
)
RAW_SCHEMA = T.StructType([T.StructField(name, T.StringType(), True) for name in RAW_FIELDS])


def _canonical_rows(records: list[dict], source_year: int, source_mode: str,
                    source_identifier: str, source_url: str) -> list[dict]:
    output = []
    for record in records:
        row = {field: None if record.get(field) is None else str(record.get(field)) for field in RAW_FIELDS}
        row.update(source_year=source_year, source_mode=source_mode,
                   source_identifier=source_identifier, source_url=source_url)
        output.append(row)
    return output


def main() -> None:
    configure_from_args()
    spark = SparkSession.builder.getOrCreate()
    cfg = load_config()
    try:
        spark.sql(f"DESCRIBE SCHEMA EXTENDED `{cfg.catalog}`.`{cfg.schema}`").collect()
    except Exception as exc:
        raise RuntimeError(
            f"Required Unity Catalog schema is unavailable: {cfg.catalog}.{cfg.schema}. "
            "Create it before running the pipeline or grant the job identity access."
        ) from exc
    stage = f"{cfg.table_prefix}._bronze_medicaid_stage"
    bronze = f"{cfg.table_prefix}.bronze_raw_medicaid_utilization"
    schema = RAW_SCHEMA.add("source_year", T.IntegerType(), False).add("source_mode", T.StringType(), False).add("source_identifier", T.StringType(), False).add("source_url", T.StringType(), False)
    client = CMSMedicaidClient(page_size=cfg.cms_page_size)
    if cfg.national_scope and cfg.cms_mode != "bulk_csv":
        raise ValueError("National MEDICAID_STATES=ALL ingestion requires CMS_MODE=bulk_csv.")
    wrote = False
    buffered_rows: list[dict] = []

    def flush() -> None:
        nonlocal wrote, buffered_rows
        if not buffered_rows:
            return
        rows_to_write, buffered_rows = buffered_rows, []
        frame = spark.createDataFrame(rows_to_write, schema=schema)
        mode = "append" if wrote else "overwrite"
        frame.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(stage)
        wrote = True

    def persist(records: list[dict], year: int, identifier: str, url: str) -> None:
        nonlocal buffered_rows
        rows = _canonical_rows(records, year, cfg.cms_mode, identifier, url)
        buffered_rows.extend(rows)
        if len(buffered_rows) >= cfg.cms_write_batch_size:
            flush()

    if cfg.cms_mode == "bulk_csv":
        try:
            spark.sql(
                f"DESCRIBE VOLUME `{cfg.catalog}`.`{cfg.schema}`.`{cfg.volume}`"
            ).collect()
        except Exception as exc:
            raise RuntimeError(
                f"Required Unity Catalog Volume is unavailable: "
                f"{cfg.catalog}.{cfg.schema}.{cfg.volume}. "
                "Create it before running the pipeline or grant the job identity access."
            ) from exc
        download_dir = Path(f"/Volumes/{cfg.catalog}/{cfg.schema}/{cfg.volume}/cms")
        frames = []
        for year in cfg.years:
            source = CMS_DATASETS[year]
            local_path = client.download_bulk_file(
                source["bulk_url"], download_dir / f"sdud_{year}.csv"
            )
            frame = spark.read.option("header", "true").option("mode", "PERMISSIVE").csv(str(local_path))
            for original_name in frame.columns:
                canonical_name = normalize_header(original_name)
                if canonical_name != original_name:
                    frame = frame.withColumnRenamed(original_name, canonical_name)
            for field in RAW_FIELDS:
                if field not in frame.columns:
                    frame = frame.withColumn(field, F.lit(None).cast("string"))
            frame = frame.select(*[F.col(field).cast("string").alias(field) for field in RAW_FIELDS])
            if cfg.states:
                frame = frame.filter(F.upper(F.trim("state")).isin(list(cfg.states)))
            frame = (
                frame.withColumn("source_year", F.lit(year).cast("int"))
                .withColumn("source_mode", F.lit("bulk_csv"))
                .withColumn("source_identifier", F.lit(source["dataset_id"]))
                .withColumn("source_url", F.lit(source["bulk_url"]))
            )
            frames.append(frame)
        combined = frames[0]
        for frame in frames[1:]:
            combined = combined.unionByName(frame)
        combined.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(stage)
        wrote = True
    else:
        for year in cfg.years:
            source = CMS_DATASETS[year]
            api_url = f"https://data.medicaid.gov/api/1/datastore/query/{source['dataset_id']}/0"
            for state in cfg.states:
                for page in client.iter_api_pages(source["dataset_id"], state):
                    persist(page, year, source["dataset_id"], api_url)

    flush()
    if not wrote:
        raise RuntimeError("CMS returned no records for the configured scope.")
    staged = spark.table(stage).withColumn(
        "record_hash",
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(name), F.lit("<NULL>")) for name in RAW_FIELDS], F.col("source_identifier")), 256),
    ).withColumn("ingested_at", F.current_timestamp()).dropDuplicates(["record_hash"])
    staged.createOrReplaceTempView("bronze_medicaid_deduplicated")
    if not spark.catalog.tableExists(bronze):
        staged.write.format("delta").mode("overwrite").saveAsTable(bronze)
    else:
        spark.sql(f"""
            MERGE INTO {bronze} target
            USING bronze_medicaid_deduplicated source
            ON target.record_hash = source.record_hash
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    spark.sql(f"DROP TABLE IF EXISTS {stage}")
    print(f"Bronze ingestion complete: {bronze}; rows={spark.table(bronze).count()}")


if __name__ == "__main__":
    main()
