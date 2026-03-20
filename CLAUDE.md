# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Minute Media home assignment: build a revenue Single Source of Truth (SSOT) consolidating 5 revenue streams into a single hourly-granularity BigQuery table. Built as an Astronomer (Airflow) project.

The assignment spec is at `include/references/Data Engineer Home Assignment - Minute Media (1) (2).docx`.

## Commands

```bash
# Start local Airflow (requires Docker)
astro dev start

# Run DAG integrity tests
astro dev pytest tests/

# Run a single SQL against BigQuery directly
bq query --use_legacy_sql=false 'SELECT ...'

# Authenticate bq CLI with the project service account
gcloud auth activate-service-account \
  --key-file="include/gcp/service-account.json" \
  --project=minute-media-490214
```

## Architecture

### Single DAG: `Model_consolidated_revenue_hourly`

One Airflow DAG (`@hourly`) orchestrates 5 sub-pipelines that feed into a single SSOT table. Each pipeline follows an incremental pattern: **increment → pull → transform → merge**.

**Execution chain (sequential merges → final validation):**
```
events_stream_merge → ssp_merge → syndication_merge → demand_partner_merge → gam_merge → validate_ssot
```

### Sub-pipelines

| Pipeline | Source Table | Role | Merge Strategy |
|---|---|---|---|
| **events_stream** | `events` (first-party) | Base layer — aggregates raw events hourly, splits estimated vs actual revenue | UPSERT (insert + update) |
| **GAM** | `gam_data_transfer` | Reconciles CPM for GAM network on O&O properties. Runs only at 17:00 UTC (time-gated) | UPDATE only — enriches existing rows |
| **demand_partner** | `demand_partner_reports` | Reconciles CPM for non-GAM/non-MinuteSSP networks on O&O. Smart delta per partner | UPDATE only — enriches existing rows |
| **SSP** | `ssp_report` | Adds external-only SSP activity (`site_type in ('external','ext_player')`) | UPSERT — inserts new rows for external activity |
| **syndication** | `syndication_revenue` | Adds syndication partner revenue (not tracked in events) | UPSERT — inserts new rows |

### Revenue Reconciliation Logic

- **MinuteSSP** CPM is actual — written directly by events_stream as `actual_revenue`
- **GAM** actual revenue is proportionally allocated to hourly event rows using `tracked_event_count` as weight
- **Demand partners** (Magnite, Triplelift, IndexExchange, etc.) use the same proportional allocation pattern, with ad-unit mapping via `dim_ad_unit_mapping`
- **SSP external** and **Syndication** daily data is exploded to 24 hourly rows via `UNNEST(generate_timestamp_array(...))`

### BigQuery Layout

- **Project:** `minute-media-490214`
- **DWH schema:** `minute_media_DWH` — fact tables and dimension tables
- **STG schema:** `minute_media_STG` — intermediate staging tables (CREATE OR REPLACE per run)
- **SSOT table:** `fact_consolidated_revenue_hourly` — partitioned by `event_date`, clustered by `organization_id, network, event, country`

### Dimension Tables Referenced

- `dim_line_items_mapping` — maps `line_item_id` → `advertiser_name`
- `dim_country_mapping` — maps full country names (GAM format) → ISO codes
- `dim_ad_unit_mapping` — maps partner-specific ad unit identifiers to DWH ad unit paths

### SQL File Convention

All SQL lives under `dags/minute_media/Model_consolidated_revenue_hourly/fact_consolidated_revenue_hourly/{pipeline_name}/`. The DAG Python file uses Airflow's `template_searchpath` pointed at the `fact_consolidated_revenue_hourly/` directory, and SQL files are loaded via `{% include 'subdir/file.sql' %}`.

SQL files use Jinja templating to pull XCom values for incremental watermarks, e.g.:
```sql
WHERE timestamp > CAST('{{ ti.xcom_pull(task_ids="events_stream_get_increment")[0][0] }}' AS TIMESTAMP)
```

### Key Design Decisions

- The SSOT table has both `estimated_revenue` (from real-time events) and `actual_revenue` (from reconciliation sources) — downstream consumers choose which to use
- `tracked_event_count` comes from events; `reconciled_event_count` comes from partner reports
- GAM and demand_partner pipelines never INSERT new rows — they only UPDATE existing event rows to prevent "ghost rows" with revenue but no tracked events
- SSP pipeline filters to external-only (`site_type in ('external','ext_player')`) to avoid double-counting with events_stream
- Merge ON clauses use `COALESCE(field, 'NA')` for nullable dimensions to handle NULL-safe joins

### Data Quality Validation (`validate_ssot`)

A single PythonOperator runs after all merges, with `trigger_rule="none_failed"`. Logic lives in `dq/validate_ssot.py`, SQL checks in `dq/tests/`.

**SSOT checks (always run, blocking):**
- `ssot_grain_uniqueness` — no duplicate rows at the 13-dimension grain
- `ssot_required_fields` — `event_date`, `event_hour`, `event`, `organization_id` never NULL (note: `network` is nullable — merge ON clauses handle it via COALESCE)
- `ssot_no_negatives` — no negative revenue or count values

**Pipeline checks (conditional, warning):**
- `ssp_revenue_conservation` — STG vs SSOT revenue match (grain-level JOIN)
- `syndication_revenue_conservation` — same pattern as SSP
- `gam_revenue_bounding` — allocated revenue ≤ source revenue (tolerance: 0.1%)
- `demand_partner_revenue_bounding` — same pattern as GAM

**Gating:** Each pipeline check runs only when its last processing task succeeded in the current DAG run (checked via `dag_run.get_task_instance()`). This avoids running checks against stale STG data when a pipeline was skipped by a ShortCircuit gate.

**ShortCircuit fix:** All 4 ShortCircuitOperators use `ignore_downstream_trigger_rules=False` to prevent cascading skips through the merge chain. GAM and demand_partner merge SQL files have Jinja XCom guards (`{% if increment is not none %}`) to handle the case where their upstream `get_increment` task was skipped.

## Gotchas

- The assignment spec is a `.docx` binary file. Use `textutil -convert txt -stdout` to read it on macOS.
- QA analysis reports live in `include/references/` (e.g., `include/references/gam_path_qa_analysis.md`, `include/references/demand_partner_path_qa_analysis.md`).
- `bq` CLI is authenticated via service account (`include/gcp/service-account.json`), project `minute-media-490214`.
- Source data is small (sample dataset) — safe to run exploratory queries without cost concern.
- `payingEntity` in the raw `events_stream` table is the actual demand partner for Prebid-won impressions (e.g. `'Triplelift'`, `'IndexExchange'`). The `network` field for these events is always `'Prebid'` (the auction platform). `events_stream_pull_data.sql` maps `payingEntity → network` for Prebid events so downstream pipelines can reconcile by partner name.
- `payingEntity='Index'` in the events table corresponds to `partner_name='IndexExchange'` in `fact_demand_partner_reports` — a known name mismatch that still needs a fix (pending approval).
- SSP and syndication explode steps spread daily values across 24 hours using `DIV(n, 24)` + `MOD(n, 24)` remainder on the first hour to guarantee exact conservation when summed.
- DAG helpers `_bq_sql_task`, `_bq_get_increment`, and `continue_if_new_data` centralize `GCP_CONN_ID` and `BQ_LOCATION` internally. Do not pass `gcp_conn_id` or `location` to task call sites.
- All 14 STG tables have `PARTITION BY` (date column). Only 7 with downstream JOINs have `CLUSTER BY` (keys matching JOIN conditions); the other 7 are passthrough tables (no clustering needed). Each table has a comment explaining the decision.

## QA Issue Trackers

- `include/references/events_path_qa_analysis.md`
- `include/references/gam_path_qa_analysis.md`
- `include/references/demand_partner_path_qa_analysis.md`
- `include/references/ssp_path_qa_analysis.md`

## Git Workflow

- Branch from `dev` for feature work: `git checkout -b feature/xxx dev`
- PRs go `feature/* → dev → main`
- Remote is `ofekhaviv1/minutemedia_assignment` on GitHub (HTTPS)
