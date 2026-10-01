-- Capstone CDF destination: bootcamp_students.merediver.
-- Run after the app has created at least one activity event and Lakebase CDF is Streaming.

SELECT event_id, session_id, event_type, tool_category, outcome, approved_write,
       created_at, _pg_change_type, _pg_lsn, _sort_by, _timestamp
FROM bootcamp_students.merediver.lb_agent_activity_events_history
ORDER BY _sort_by DESC
LIMIT 50;

SELECT *
FROM bootcamp_students.merediver.gold_agent_activity_daily
ORDER BY activity_date DESC, event_type, tool_category;

SELECT *
FROM bootcamp_students.merediver.gold_workflow_changes_daily
ORDER BY activity_date DESC;
