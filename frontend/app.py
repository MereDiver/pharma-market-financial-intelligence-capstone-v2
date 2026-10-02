"""Databricks App frontend for management KPIs, Agent analysis, and saved work."""

from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from flask import Flask, jsonify, render_template, request

import agent_client
import analytics_client
import lakebase

logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))
logger = logging.getLogger("pharma-finance-frontend")
app = Flask(__name__)


def _session_id(payload: dict[str, Any]) -> str:
    raw = payload.get("session_id")
    if raw is None:
        return str(uuid4())
    try:
        return str(UUID(str(raw)))
    except (TypeError, ValueError) as exc:
        raise ValueError("session_id must be a valid UUID.") from exc


def _record_activity(*args: Any, **kwargs: Any) -> None:
    try:
        lakebase.record_activity(*args, **kwargs)
    except Exception:
        logger.exception("Activity event could not be recorded")


def safe(value: Any) -> Any:
    if isinstance(value, (date, datetime)): return value.isoformat()
    if isinstance(value, Decimal): return float(value)
    if isinstance(value, dict): return {key: safe(item) for key, item in value.items()}
    if isinstance(value, list): return [safe(item) for item in value]
    return value


@app.get("/")
def index() -> str:
    return render_template("index.html")


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.get("/api/dashboard")
def dashboard():
    year = int(request.args.get("year", "2025"))
    quarter = int(request.args["quarter"]) if request.args.get("quarter") else None
    state = request.args.get("state") or None
    return jsonify({"status": "success", **safe(analytics_client.dashboard(year, quarter, state))})


@app.get("/api/evidence")
def evidence():
    return jsonify({
        "status": "success",
        "pipeline": safe(analytics_client.data_evidence()),
        "lakebase": safe(lakebase.evidence_counts()),
    })


@app.get("/api/activity")
def activity():
    try:
        return jsonify({"status": "success", **safe(analytics_client.activity_summary())})
    except Exception:
        logger.exception("CDF activity analytics are not available")
        return jsonify({
            "status": "pending",
            "message": "Run the Lakebase CDF analytics pipeline after the first application events.",
        })


@app.post("/api/agent")
def agent():
    payload = request.get_json(silent=True) or {}
    session_id = _session_id(payload)
    started = time.monotonic()
    _record_activity(session_id, "agent_request", "unknown", "pending")
    try:
        result = agent_client.ask_agent(
            payload.get("message", ""),
            payload.get("previous_response_id"),
        )
        elapsed = int((time.monotonic() - started) * 1000)
        if result.get("approval_required"):
            _record_activity(session_id, "approval_requested", "write", "pending", latency_ms=elapsed)
        else:
            _record_activity(session_id, "agent_response", "retrieval", "success", latency_ms=elapsed)
        return jsonify({"status": "success", "session_id": session_id, **safe(result)})
    except Exception as exc:
        _record_activity(
            session_id, "error", "unknown", "error",
            latency_ms=int((time.monotonic() - started) * 1000), error_type=type(exc).__name__,
        )
        raise


@app.post("/api/agent/approval")
def agent_approval():
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload.get("approve"), bool):
        raise ValueError("approve must be true or false.")
    session_id = _session_id(payload)
    started = time.monotonic()
    try:
        result = agent_client.continue_agent(payload.get("approval_token", ""), payload["approve"])
        _record_activity(
            session_id,
            "approval_granted" if payload["approve"] else "approval_rejected",
            "write",
            "success" if payload["approve"] else "cancelled",
            latency_ms=int((time.monotonic() - started) * 1000),
            approved_write=payload["approve"],
        )
        return jsonify({"status": "success", "session_id": session_id, **safe(result)})
    except Exception as exc:
        _record_activity(
            session_id, "error", "write", "error",
            latency_ms=int((time.monotonic() - started) * 1000), error_type=type(exc).__name__,
        )
        raise


@app.get("/api/workspace")
def workspace():
    collections = {}
    errors = {}
    for name, loader in (("investigations", lakebase.investigations),
                         ("actions", lakebase.actions), ("notes", lakebase.notes)):
        try:
            collections[name] = safe(loader())
        except Exception:
            logger.exception("Workspace collection unavailable: %s", name)
            collections[name] = []
            errors[name] = f"{name.replace('_', ' ').title()} access is not configured."
    return jsonify({"status": "success", **collections, "errors": errors})


@app.post("/api/investigations/<investigation_id>/notes")
def add_note(investigation_id: str):
    payload = request.get_json(silent=True) or {}
    session_id = _session_id(payload)
    note_id = lakebase.add_note(investigation_id, payload.get("note_text", ""))
    _record_activity(session_id, "note_created", "frontend", "success", approved_write=True)
    return jsonify({"status": "success", "session_id": session_id, "note_id": note_id}), 201


@app.post("/api/actions/<action_id>/complete")
def complete_action(action_id: str):
    payload = request.get_json(silent=True) or {}
    session_id = _session_id(payload)
    changed = lakebase.complete_action(action_id)
    if changed:
        _record_activity(session_id, "action_completed", "frontend", "success", approved_write=True)
    return jsonify({"status": "success" if changed else "not_found", "session_id": session_id}), (200 if changed else 404)


@app.errorhandler(Exception)
def error_handler(error: Exception):
    logger.exception("Frontend request failed")
    if isinstance(error, ValueError):
        return jsonify({"status": "error", "message": str(error)}), 400
    return jsonify({"status": "error", "message": "The intelligence workspace is temporarily unavailable. Verify attached resources."}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("DATABRICKS_APP_PORT", "8001")))
