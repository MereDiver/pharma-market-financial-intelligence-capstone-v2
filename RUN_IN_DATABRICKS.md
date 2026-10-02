# Databricks Execution and Deployment Runbook

Complete these steps in order. Replace example identifiers with your workspace values. Never add passwords, tokens, endpoint credentials, or real `.env` files to Git or the submission ZIP.

## 1. Prepare the workspace

1. Put this repository in a Databricks Git folder.
2. Use the bootcamp-provided Unity Catalog catalog `bootcamp_students`.
3. Use the existing student schema `merediver`. The pipeline never creates a catalog; it only uses this existing catalog/schema and idempotently verifies the schema.
4. The pipeline does not create a catalog, schema, or Volume. Verify the existing landing Volume:

```sql
CREATE VOLUME IF NOT EXISTS bootcamp_students.merediver.pharma_pipeline;
```

The command above is only needed if the Volume does not already exist. The ingestion job now performs read-only `DESCRIBE` checks and fails clearly if the existing schema or Volume is unavailable.

5. Create or select a serverless SQL warehouse.
6. Create or select a Lakebase project, branch, endpoint, and database.
7. In Lakebase SQL Editor, enable pgvector:

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

8. Keep `MEDICAID_STATES=ALL` and `CMS_MODE=bulk_csv` for the final run.
9. `mcp_server/app.yaml` and `frontend/app.yaml` are already configured with `CATALOG=bootcamp_students` and `SCHEMA=merediver`.

## 2. Configure the Lakebase endpoint

Copy the full resource name from the Lakebase endpoint page. Pass it at deploy time or store it only in the ignored local file `.databricks/bundle/dev/variable-overrides.json`:

```json
{
  "lakebase_endpoint_name": "projects/PROJECT/branches/BRANCH/endpoints/ENDPOINT",
  "catalog": "bootcamp_students",
  "schema": "merediver"
}
```

Do not commit this file.

## 3. Validate and deploy the bundle

From the repository root in a Databricks terminal or configured local terminal:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

The bundle creates the national Spark workflow, the Lakeflow activity pipeline, and a job that refreshes that pipeline.

## 4. Run the national Spark pipeline

```bash
databricks bundle run -t dev pharma_intelligence_pipeline
```

The final run streams both annual CMS files to the configured Volume and then processes them with Spark. It can take materially longer than the old five-state Free Edition run. Do not switch to a small subset merely to produce a green screenshot.

When it completes, run:

```sql
SELECT *
FROM bootcamp_students.merediver.gold_data_quality_summary
ORDER BY refreshed_at DESC;

SELECT year, COUNT(*) AS rows, COUNT(DISTINCT state) AS jurisdictions
FROM bootcamp_students.merediver.silver_medicaid_utilization_clean
GROUP BY year
ORDER BY year;

SELECT match_status, has_label_document, COUNT(*) AS products
FROM bootcamp_students.merediver.openfda_product_enrichment
GROUP BY match_status, has_label_document
ORDER BY match_status, has_label_document;
```

Pass conditions:

- Bronze row count is above one million.
- The measured scope is national/all available jurisdictions.
- Gold tables contain records.
- openFDA results show live matched/fallback/unmatched outcomes.
- At least one real FDA label document exists. If none exists, diagnose live responses and increase `max_openfda_products` before rerunning.

## 5. Verify Lakebase

The openFDA task runs the idempotent schema initializer. In Lakebase SQL Editor:

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'pharma_intelligence'
ORDER BY table_name;

SELECT n.nspname AS table_schema, c.relname AS table_name,
       CASE c.relreplident WHEN 'f' THEN 'full' ELSE c.relreplident::text END AS replica_identity
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r' AND n.nspname = 'pharma_intelligence'
ORDER BY c.relname;

SELECT COUNT(*) AS documents FROM pharma_intelligence.drug_documents;
SELECT COUNT(*) AS vector_chunks FROM pharma_intelligence.drug_embeddings;
```

The operational tables must show full replica identity.

## 6. Deploy the MCP App

1. Open **Compute > Apps** and create `mcp-pharma-intelligence` from `mcp_server/`.
2. Add the Lakebase database resource with key `postgres`.
3. Add the SQL warehouse resource with key `sql-warehouse` and **Can use**.
4. Confirm `CATALOG`, `SCHEMA`, and `APP_SCHEMA` in `mcp_server/app.yaml`.
5. Deploy the App.
6. Grant its service principal `USE CATALOG`, `USE SCHEMA`, and `SELECT` on the analytical tables. Grant required DML permissions in the dedicated Lakebase schema, but no write permission on Gold facts.
7. Verify `<MCP_APP_URL>/mcp`.

## 7. Configure Agent Bricks

1. Register the MCP endpoint as a custom MCP server, or select the MCP App directly if offered.
2. Create a Supervisor/Agent Bricks agent.
3. Enable every tool in `agent/tool_manifest.md`.
4. Paste `agent/system_prompt.md` as the instructions.
5. Test `agent/demo_questions.md`.
6. Ask the agent to save an investigation and create a follow-up.
7. Confirm it requests approval before the write.
8. Deploy the Agent endpoint.

## 8. Deploy the frontend App

1. Create `pharma-finance-intelligence` from `frontend/`.
2. Add the Agent endpoint with resource key `finance-agent` and **Can query**.
3. Add the Lakebase database with resource key `postgres`.
4. Add the SQL warehouse with resource key `sql-warehouse` and **Can use**.
5. Confirm catalog/schema values in `frontend/app.yaml`.
6. Grant the App principal `SELECT` on required Delta tables and limited Lakebase access for workspace reads, note inserts, action-status updates, and activity-event inserts.
7. Deploy and open the App.
8. Verify `/health` returns `{"status":"ok"}`.

## 9. Demonstrate the full workflow

In one App session:

1. Ask: `Which products drove the national 2025 reimbursement change versus 2024?`
2. Ask for FDA label context for one enriched product.
3. Ask: `Save this investigation and create a high-priority follow-up to review the largest driver.`
4. Inspect the approval card and click **Approve and continue**.
5. Confirm the investigation and follow-up appear.
6. Add a controller note.
7. Mark the follow-up completed.

Verify Lakebase:

```sql
SELECT investigation_id, title, status, created_at
FROM pharma_intelligence.investigations
ORDER BY created_at DESC;

SELECT action_id, action_text, priority, status, updated_at
FROM pharma_intelligence.follow_up_actions
ORDER BY created_at DESC;

SELECT event_type, tool_category, outcome, approved_write, latency_ms, created_at
FROM pharma_intelligence.agent_activity_events
ORDER BY created_at DESC;
```

## 10. Start Lakebase CDF

1. Ask a workspace admin to enable **Lakebase Change Data Feed** under **Settings > Previews** if needed.
2. Open **Lakebase Postgres** from the app switcher.
3. Select the project and branch.
4. Open **Branch overview > Lakebase CDF**.
5. Click **Start**.
6. Choose the source database and `pharma_intelligence` schema.
7. Choose destination catalog `bootcamp_students` and destination schema `merediver`.
8. Start the feed and wait for `Streaming` status.

CDF creates `lb_<table_name>_history` tables. Empty tables are skipped until their first row exists, which is why the App workflow comes first.

## 11. Run the CDF analytics pipeline

```bash
databricks bundle run -t dev pharma_activity_analytics_refresh
```

If an earlier pipeline attempt already created its output tables without the
`timestampNtz` Delta feature, use a SQL warehouse to run the following for each
table that exists, then restart the failed update:

```sql
ALTER TABLE bootcamp_students.merediver.silver_agent_activity_events
SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported');

ALTER TABLE bootcamp_students.merediver.gold_agent_activity_daily
SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported');

ALTER TABLE bootcamp_students.merediver.silver_workflow_changes
SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported');

ALTER TABLE bootcamp_students.merediver.gold_workflow_changes_daily
SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported');
```

The pipeline declares this table feature for new deployments. The manual SQL is
only a recovery step for output tables created before that declaration was
added. If one statement returns `TABLE_OR_VIEW_NOT_FOUND`, skip that table; the
next pipeline update will create it with the required property.

Run `sql/cdf_verification.sql` after replacing the example catalog/schema. Verify that:

- `lb_agent_activity_events_history` contains inserts.
- `gold_agent_activity_daily` contains daily metrics.
- `gold_workflow_changes_daily` contains investigation/follow-up changes.
- Refreshing the App displays the CDF event and approved-write metrics.

## 12. Capture evidence and build the ZIP

Follow `evidence/README.md`. Prioritize:

- App URL, measured CMS row count, FDA documents, agent answer, and saved work.
- Agent write approval and the resulting Lakebase records.
- Successful Spark job and `gold_data_quality_summary`.
- CDF history with `_pg_change_type`, `_pg_lsn`, `_sort_by`, and `_timestamp`.
- Successful Lakeflow update and daily Gold activity.

Add only genuine screenshots to `evidence/`, then run:

```bash
python scripts/build_submission_zip.py
```

Open the ZIP and confirm it contains source, `CAPSTONE_WRITEUP.md`, this runbook, and evidence. It must not contain `.git`, caches, raw CMS files, model downloads, credentials, or videos.
