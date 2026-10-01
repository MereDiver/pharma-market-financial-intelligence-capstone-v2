"""Read/operational-update access using fresh Lakebase OAuth credentials."""

from __future__ import annotations

import os
import re
from contextlib import contextmanager
from typing import Any, Iterator
from uuid import UUID, uuid4

import psycopg2
from databricks.sdk import WorkspaceClient
from psycopg2 import sql
from psycopg2.extras import RealDictCursor


def schema_name() -> str:
    value = os.getenv("APP_SCHEMA", "pharma_intelligence")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise RuntimeError("Invalid APP_SCHEMA.")
    return value


@contextmanager
def connection() -> Iterator[Any]:
    client = WorkspaceClient()
    values = {"host": os.getenv("PGHOST"), "dbname": os.getenv("PGDATABASE"),
              "port": os.getenv("PGPORT"), "sslmode": os.getenv("PGSSLMODE"),
              "user": os.getenv("PGUSER"), "endpoint": os.getenv("ENDPOINT_NAME")}
    if any(not value for value in values.values()):
        raise RuntimeError("Lakebase App resource configuration is incomplete.")
    endpoint = values.pop("endpoint")
    values["port"] = int(values["port"])
    credential = client.postgres.generate_database_credential(endpoint=endpoint)
    temporary_credential = getattr(credential, "token", None)
    if not temporary_credential:
        raise RuntimeError("No temporary Lakebase credential was returned.")
    db = psycopg2.connect(**values, password=temporary_credential,
                          cursor_factory=RealDictCursor, connect_timeout=15)
    try:
        yield db
    finally:
        db.close()


def query(statement: Any, params: tuple = ()) -> list[dict[str, Any]]:
    with connection() as db:
        with db.cursor() as cursor:
            cursor.execute(statement, params)
            return [dict(row) for row in cursor.fetchall()]


def investigations() -> list[dict[str, Any]]:
    return query(sql.SQL("SELECT investigation_id,title,question,summary,status,created_at FROM {}.investigations ORDER BY created_at DESC LIMIT 50").format(sql.Identifier(schema_name())))


def actions() -> list[dict[str, Any]]:
    return query(sql.SQL("SELECT action_id,investigation_id,action_text,priority,status,due_date,created_at FROM {}.follow_up_actions ORDER BY created_at DESC LIMIT 50").format(sql.Identifier(schema_name())))


def notes() -> list[dict[str, Any]]:
    schema = sql.Identifier(schema_name())
    statement = sql.SQL(
        "SELECT n.note_id,n.investigation_id,n.note_text,n.author,n.created_at,"
        "i.title AS investigation_title FROM {}.analyst_notes n "
        "JOIN {}.investigations i ON i.investigation_id=n.investigation_id "
        "ORDER BY n.created_at DESC LIMIT 50"
    ).format(schema, schema)
    return query(statement)


def evidence_counts() -> dict[str, Any]:
    schema = sql.Identifier(schema_name())
    statement = sql.SQL(
        "SELECT (SELECT COUNT(*) FROM {}.drug_documents) AS drug_document_count,"
        "(SELECT COUNT(*) FROM {}.drug_embeddings) AS embedding_count,"
        "(SELECT COUNT(*) FROM {}.investigations) AS investigation_count,"
        "(SELECT COUNT(*) FROM {}.follow_up_actions) AS follow_up_count,"
        "(SELECT COUNT(*) FROM {}.agent_activity_events) AS activity_event_count"
    ).format(schema, schema, schema, schema, schema)
    rows = query(statement)
    return rows[0] if rows else {}


def add_note(investigation_id: str, note_text: str, author: str = "Frontend controller") -> str:
    normalized_id = str(UUID(str(investigation_id)))
    normalized_note = " ".join(str(note_text or "").split())
    if not normalized_note or len(normalized_note) > 10000:
        raise ValueError("Note must be between 1 and 10000 characters.")
    note_id = str(uuid4())
    statement = sql.SQL(
        "INSERT INTO {}.analyst_notes (note_id,investigation_id,note_text,author) "
        "VALUES (%s,%s,%s,%s)"
    ).format(sql.Identifier(schema_name()))
    with connection() as db:
        try:
            with db.cursor() as cursor:
                cursor.execute(statement, (note_id, normalized_id, normalized_note, author))
            db.commit()
        except Exception:
            db.rollback()
            raise
    return note_id


def complete_action(action_id: str) -> bool:
    normalized = str(UUID(action_id))
    with connection() as db:
        try:
            with db.cursor() as cursor:
                cursor.execute(sql.SQL("UPDATE {}.follow_up_actions SET status='completed',updated_at=now() WHERE action_id=%s AND status='open'").format(sql.Identifier(schema_name())), (normalized,))
                changed = cursor.rowcount > 0
            db.commit()
            return changed
        except Exception:
            db.rollback(); raise


def record_activity(
    session_id: str,
    event_type: str,
    tool_category: str,
    outcome: str,
    *,
    latency_ms: int | None = None,
    approved_write: bool = False,
    error_type: str | None = None,
) -> str:
    """Append a privacy-minimal operational event for Lakebase CDF analytics."""
    allowed_events = {
        "agent_request", "agent_response", "approval_requested", "approval_granted",
        "approval_rejected", "note_created", "action_completed", "error",
    }
    allowed_categories = {"analytics", "retrieval", "write", "frontend", "unknown"}
    allowed_outcomes = {"success", "error", "pending", "cancelled"}
    if event_type not in allowed_events or tool_category not in allowed_categories or outcome not in allowed_outcomes:
        raise ValueError("Invalid activity event classification.")
    normalized_session = str(UUID(str(session_id)))
    if latency_ms is not None and (not isinstance(latency_ms, int) or latency_ms < 0):
        raise ValueError("latency_ms must be a non-negative integer.")
    event_id = str(uuid4())
    statement = sql.SQL(
        "INSERT INTO {}.agent_activity_events "
        "(event_id,session_id,event_type,tool_category,outcome,latency_ms,approved_write,error_type) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
    ).format(sql.Identifier(schema_name()))
    with connection() as db:
        try:
            with db.cursor() as cursor:
                cursor.execute(statement, (
                    event_id, normalized_session, event_type, tool_category, outcome,
                    latency_ms, bool(approved_write), str(error_type)[:120] if error_type else None,
                ))
            db.commit()
        except Exception:
            db.rollback()
            raise
    return event_id
