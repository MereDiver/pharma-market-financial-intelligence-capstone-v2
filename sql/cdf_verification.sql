-- Replace workspace.pharma_market_intelligence if your configured catalog/schema differ.
-- Run after the app has created at least one activity event and Lakebase CDF is Streaming.

SELECT event_id, session_id, event_type, tool_category, outcome, approved_write,
       created_at, _pg_change_type, _pg_lsn, _sort_by, _timestamp
FROM workspace.pharma_market_intelligence.lb_agent_activity_events_history
ORDER BY _sort_by DESC
LIMIT 50;

SELECT *
FROM workspace.pharma_market_intelligence.gold_agent_activity_daily
ORDER BY activity_date DESC, event_type, tool_category;

SELECT *
FROM workspace.pharma_market_intelligence.gold_workflow_changes_daily
ORDER BY activity_date DESC;
