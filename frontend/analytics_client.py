"""Read-only KPI queries through the attached SQL Warehouse."""

from __future__ import annotations

import os
import re
import time
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementParameterListItem, StatementState

STATE_CODE = re.compile(r"^[A-Z]{2}$")


def _table(name: str) -> str:
    allowed = {
        "quarterly": "gold_drug_performance_quarterly",
        "yoy": "gold_drug_performance_yoy",
        "quality": "gold_data_quality_summary",
        "openfda": "openfda_product_enrichment",
        "activity": "gold_agent_activity_daily",
        "workflow": "gold_workflow_changes_daily",
    }
    catalog, schema = os.getenv("CATALOG", "workspace"), os.getenv("SCHEMA", "pharma_market_intelligence")
    if name not in allowed or not catalog.replace("_", "a").isalnum() or not schema.replace("_", "a").isalnum():
        raise ValueError("Invalid governed table configuration.")
    return f"`{catalog}`.`{schema}`.`{allowed[name]}`"


def _execute(statement: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    warehouse = os.getenv("WAREHOUSE_ID")
    if not warehouse:
        raise RuntimeError("WAREHOUSE_ID is not configured.")
    client = WorkspaceClient()
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse, statement=statement,
        parameters=[StatementParameterListItem(name=k, value=str(v)) for k, v in params.items()],
        disposition=Disposition.INLINE, format=Format.JSON_ARRAY, wait_timeout="10s",
    )
    deadline = time.monotonic() + 45
    while response.status and response.status.state in {StatementState.PENDING, StatementState.RUNNING}:
        if time.monotonic() > deadline:
            raise RuntimeError("Dashboard SQL query timed out.")
        time.sleep(0.5)
        response = client.statement_execution.get_statement(response.statement_id)
    if not response.status or response.status.state != StatementState.SUCCEEDED:
        raise RuntimeError("Dashboard SQL query failed.")
    columns = [column.name for column in response.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in (response.result.data_array or [])]


def dashboard(year: int, quarter: int | None, state: str | None) -> dict[str, Any]:
    if year not in {2024, 2025} or quarter not in {None, 1, 2, 3, 4}:
        raise ValueError("Unsupported dashboard period.")
    if state and not STATE_CODE.fullmatch(state):
        raise ValueError("State must be a two-letter jurisdiction code.")
    clauses, params = ["year=CAST(:year AS INT)"], {"year": year}
    yoy_clauses, yoy_params = ["current_year=CAST(:year AS INT)"], {"year": year}
    if quarter:
        clauses.append("quarter=CAST(:quarter AS INT)"); yoy_clauses.append("quarter=CAST(:quarter AS INT)")
        params["quarter"] = quarter; yoy_params["quarter"] = quarter
    if state:
        clauses.append("state=:state"); yoy_clauses.append("state=:state")
        params["state"] = state; yoy_params["state"] = state
    where, yoy_where = " AND ".join(clauses), " AND ".join(yoy_clauses)
    kpis = _execute(f"""
      SELECT SUM(total_reimbursement) total_reimbursement,SUM(prescriptions) prescriptions,
             SUM(units_reimbursed) units_reimbursed,
             CASE WHEN SUM(prescriptions)<>0 THEN SUM(total_reimbursement)/SUM(prescriptions) END reimbursement_per_prescription
      FROM {_table('quarterly')} WHERE {where}
    """, params)
    movers = _execute(f"""
      SELECT product_key,MAX(display_product_name) display_product_name,SUM(reimbursement_change) contribution
      FROM {_table('yoy')} WHERE {yoy_where} GROUP BY product_key ORDER BY ABS(contribution) DESC LIMIT 12
    """, yoy_params) if year == 2025 else []
    yoy = _execute(f"""
      SELECT SUM(reimbursement_change) reimbursement_change,
             CASE WHEN SUM(prior_total_reimbursement)<>0 THEN SUM(reimbursement_change)/SUM(prior_total_reimbursement) END reimbursement_change_percent
      FROM {_table('yoy')} WHERE {yoy_where}
    """, yoy_params) if year == 2025 else []
    return {"kpis": kpis[0] if kpis else {}, "yoy": yoy[0] if yoy else {},
            "positive_movers": [row for row in movers if float(row["contribution"]) >= 0][:6],
            "negative_movers": [row for row in movers if float(row["contribution"]) < 0][:6]}


def data_evidence() -> dict[str, Any]:
    rows = _execute(f"""
      SELECT q.bronze_row_count,q.silver_row_count,q.jurisdiction_count,q.product_count,
             q.suppressed_row_count,q.min_year,q.max_year,q.scope,q.refreshed_at,
             e.enriched_product_count,e.label_document_count
      FROM {_table('quality')} q
      CROSS JOIN (
        SELECT COUNT(*) enriched_product_count,
               SUM(CASE WHEN has_label_document THEN 1 ELSE 0 END) label_document_count
        FROM {_table('openfda')}
      ) e
      ORDER BY q.refreshed_at DESC LIMIT 1
    """, {})
    return rows[0] if rows else {}


def activity_summary() -> dict[str, Any]:
    activity = _execute(f"""
      SELECT COALESCE(SUM(event_count),0) event_count,
             COALESCE(SUM(success_count),0) success_count,
             COALESCE(SUM(error_count),0) error_count,
             COALESCE(SUM(approved_write_count),0) approved_write_count,
             COALESCE(SUM(distinct_sessions),0) session_days,
             CASE WHEN SUM(event_count)>0 THEN SUM(weighted_latency_ms)/SUM(event_count) END avg_latency_ms,
             MAX(last_event_at) last_event_at
      FROM {_table('activity')}
      WHERE activity_date >= date_sub(current_date(),30)
    """, {})
    workflow = _execute(f"""
      SELECT COALESCE(SUM(investigation_changes),0) investigation_changes,
             COALESCE(SUM(follow_up_changes),0) follow_up_changes,
             MAX(last_change_at) last_change_at
      FROM {_table('workflow')}
      WHERE activity_date >= date_sub(current_date(),30)
    """, {})
    return {"agent": activity[0] if activity else {}, "workflow": workflow[0] if workflow else {}}
