# Capstone Delivery Write-up

## Problem and solution

Pharma Finance, Market Access, and Commercial Analytics teams often investigate public utilization signals separately from regulatory product context, then lose the result in an untracked conversation. Pharma Market & Financial Intelligence 2.0 provides one Databricks workflow for measuring Medicaid reimbursement and utilization changes, retrieving relevant FDA label context, saving an evidence-grounded investigation, and managing follow-up work.

The system deliberately distinguishes public Medicaid reimbursement from manufacturer revenue, net sales, profit, list price, or realized pricing. It also treats FDA labels as product context rather than clinical advice.

## Data and Spark

The Spark workflow ingests the official 2024 and 2025 CMS State Drug Utilization bulk files. Those sources contain an expected 10,616,954 records before pipeline validation and therefore exceed the one-million-row Volume threshold. The application displays the measured Bronze/Silver counts and jurisdiction count from `gold_data_quality_summary`; the expected source total is not presented as execution proof.

The national ingest streams each official file once to a Unity Catalog Volume and then uses distributed Spark CSV processing. Bronze preserves raw strings, source metadata, ingestion time, and deterministic hashes. Silver normalizes NDCs, types measures, keeps suppressed values null, rejects malformed periods, and deduplicates records. Gold builds quarterly, YoY, state, and portfolio marts plus an evidence table. Writes are re-runnable: completed downloads are reused, Bronze merges by record hash, and downstream tables are deterministically rebuilt.

openFDA supplies the third-party API integration. The client uses bounded requests, timeouts, retries for 429/5xx responses, malformed-JSON validation, and explicit unmatched states. Product metadata and Drug Label sections are stored with provenance. Label narratives are hashed, chunked, embedded, indexed in Lakebase pgvector, retrieved by the agent, and surfaced in the application. This demonstrates Variety through structured CSV/JSON and meaningful processing of unstructured text.

## Lakebase and agent actions

Delta/Unity Catalog owns analytical facts. Lakebase owns mutable application state: investigations, findings, analyst notes, follow-up actions, and privacy-minimal agent activity events. Foreign keys, constraints, timestamps, indexes, and replica identity protect the relational model and prepare it for CDF.

Agent Bricks calls governed MCP tools rather than arbitrary SQL. Retrieval tools access Gold metrics, openFDA profiles, and vector-search results. Write tools create investigations, findings, notes, and follow-up actions or update permitted workflow statuses. The frontend automatically continues read-only calls but presents the complete proposed write arguments for explicit approval before a protected action executes.

## Operational analytics

The frontend appends activity events to Lakebase without storing prompt text. Lakebase CDF replicates operational changes into immutable Unity Catalog history tables. The Lakeflow pipeline reads those tables incrementally and materializes daily agent and workflow analytics, including events, success/error counts, latency, sessions, approved writes, and workflow changes.

Volume and Variety are the two required Big Data Vs. CDF can additionally demonstrate sub-minute operational latency, but the submission does not depend on a Velocity claim unless measured evidence is captured in the target workspace.

## Scope decision

The proposal described SEC EDGAR as an extension. The final pass-focused implementation defers that stretch module because openFDA label processing already provides unstructured Variety and the existing CMS/openFDA/Lakebase/agent workflow covers every required rubric component. The decision reduces deployment risk while retaining the proposal's core architecture and user value.

## Evidence policy

The repository never treats code presence as proof of deployment. Final submission evidence must include successful Spark and Lakeflow runs, measured row/document counts, live openFDA results, Lakebase writes, CDF history, a deployed App URL, an agent retrieval response, and an approved write reflected in the UI.
