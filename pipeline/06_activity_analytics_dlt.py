"""Lakeflow pipeline: Lakebase CDF histories to analytics-ready Delta tables."""

from __future__ import annotations

import re

import dlt
from pyspark.sql import functions as F


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _configured_identifier(name: str, default: str) -> str:
    value = spark.conf.get(name, default).strip()
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{name} must be an unquoted Unity Catalog identifier.")
    return value


SOURCE_CATALOG = _configured_identifier("source_catalog", "bootcamp_students")
SOURCE_SCHEMA = _configured_identifier("source_schema", "merediver")


def _source(table: str) -> str:
    if not _IDENTIFIER.fullmatch(table):
        raise ValueError("Invalid CDF source table name.")
    return f"`{SOURCE_CATALOG}`.`{SOURCE_SCHEMA}`.`{table}`"


@dlt.table(
    name="silver_agent_activity_events",
    comment="Validated application events streamed from Lakebase Change Data Feed.",
)
@dlt.expect_or_drop("valid_change_type", "_pg_change_type IN ('insert', 'update_postimage')")
@dlt.expect_or_drop("valid_event_type", "event_type IS NOT NULL")
def silver_agent_activity_events():
    return (
        spark.readStream.table(_source("lb_agent_activity_events_history"))
        .filter(F.col("_pg_change_type").isin("insert", "update_postimage"))
        .select(
            "event_id",
            "session_id",
            "event_type",
            "tool_category",
            "outcome",
            F.coalesce(F.col("latency_ms"), F.lit(0)).cast("long").alias("latency_ms"),
            "approved_write",
            "error_type",
            "created_at",
            "_pg_lsn",
            "_pg_xid",
            "_sort_by",
            "_timestamp",
        )
        .withColumn("activity_date", F.to_date("created_at"))
    )


@dlt.table(
    name="gold_agent_activity_daily",
    comment="Daily application usage, outcomes, latency, sessions, and approved writes.",
)
def gold_agent_activity_daily():
    return (
        dlt.read("silver_agent_activity_events")
        .groupBy("activity_date", "event_type", "tool_category")
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("outcome") == "success", 1).otherwise(0)).alias("success_count"),
            F.sum(F.when(F.col("outcome") == "error", 1).otherwise(0)).alias("error_count"),
            F.sum(F.when(F.col("approved_write"), 1).otherwise(0)).alias("approved_write_count"),
            F.countDistinct("session_id").alias("distinct_sessions"),
            F.sum("latency_ms").alias("weighted_latency_ms"),
            F.max("created_at").alias("last_event_at"),
        )
    )


@dlt.table(
    name="silver_workflow_changes",
    comment="Investigation and follow-up state changes captured from Lakebase CDF.",
)
def silver_workflow_changes():
    investigations = (
        spark.readStream.table(_source("lb_investigations_history"))
        .filter(F.col("_pg_change_type").isin("insert", "update_postimage", "delete"))
        .select(
            F.lit("investigation").alias("entity_type"),
            F.col("investigation_id").cast("string").alias("entity_id"),
            "_pg_change_type",
            "_sort_by",
            "_timestamp",
        )
    )
    follow_ups = (
        spark.readStream.table(_source("lb_follow_up_actions_history"))
        .filter(F.col("_pg_change_type").isin("insert", "update_postimage", "delete"))
        .select(
            F.lit("follow_up").alias("entity_type"),
            F.col("action_id").cast("string").alias("entity_id"),
            "_pg_change_type",
            "_sort_by",
            "_timestamp",
        )
    )
    return investigations.unionByName(follow_ups).withColumn("activity_date", F.to_date("_timestamp"))


@dlt.table(
    name="gold_workflow_changes_daily",
    comment="Daily Lakebase investigation and follow-up changes for operational analytics.",
)
def gold_workflow_changes_daily():
    return (
        dlt.read("silver_workflow_changes")
        .groupBy("activity_date")
        .agg(
            F.sum(F.when(F.col("entity_type") == "investigation", 1).otherwise(0)).alias("investigation_changes"),
            F.sum(F.when(F.col("entity_type") == "follow_up", 1).otherwise(0)).alias("follow_up_changes"),
            F.sum(F.when(F.col("_pg_change_type") == "delete", 1).otherwise(0)).alias("delete_count"),
            F.max("_timestamp").alias("last_change_at"),
        )
    )
