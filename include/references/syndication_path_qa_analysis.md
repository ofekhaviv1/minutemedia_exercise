# Syndication Path — Issue Tracker

> Reference file for documenting and tracking issues found during QA of the syndication pipeline.

## Table of Contents

- [TODO List](#todo-list)
- [Issue Details](#issue-details)
  - [#1: Cross-pipeline ShortCircuit skip propagates to downstream merges](#1-cross-pipeline-shortcircuit-skip-propagates-to-downstream-merges)
  - [#2: Daily revenue and article_count multiplied ×24 when exploded to hourly rows](#2-daily-revenue-and-article_count-multiplied-24-when-exploded-to-hourly-rows)
  - [#3: check_new_data uses strict > but pull_data uses >= (watermark boundary inconsistency)](#3-check_new_data-uses-strict--but-pull_data-uses--watermark-boundary-inconsistency)
  - [#4: Historical re-delivery of past dates silently ignored](#4-historical-re-delivery-of-past-dates-silently-ignored)
  - [#5: Partition pruning in merge uses fixed -1 day offset from watermark](#5-partition-pruning-in-merge-uses-fixed--1-day-offset-from-watermark)
- [Validation Queries](#validation-queries)

---

## TODO List

| # | Priority | Grade | Area | Task / File | Problem | Status |
|---|----------|-------|------|-------------|---------|--------|
| 1 | Critical | 10 | Orchestration | `Model_consolidated_revenue_hourly.py` (line 480) | Cross-pipeline ShortCircuit skips propagate downstream: SSP or syndication no-new-data → demand_partner and GAM merges silently skipped | ✅ Fixed |
| 2 | Critical | 10 | Data Correctness | `syndication_explode_data` / `syndication_explode_data.sql` | Daily `total_revenue` and `article_count` assigned unchanged to all 24 hourly rows — SSOT SUM per date returns 24× actual value | ✅ Fixed |
| 3 | Medium | 5 | Incremental | `syndication_check_new_data.sql` vs `syndication_pull_data.sql` | check_new_data uses strict `>` watermark but pull_data uses `>=` — watermark date always re-pulled on every run with new data | ⚠️ Accepted Risk |
| 4 | Medium | 4 | Incremental | `syndication_check_new_data` / `syndication_check_new_data.sql` | Vendor re-delivery for dates before the current watermark is silently ignored — no backward-looking window | ⚠️ Accepted Risk |
| 5 | Low | 2 | Cost | `syndication_merge` / `syndication_merge.sql` | Partition pruning uses watermark minus 1 day; multi-date backfills skip UPDATE for old target rows | ⚠️ Accepted Risk |

---

## Issue Details

### #1: Cross-pipeline ShortCircuit skip propagates to downstream merges

**Task / File:** `Model_consolidated_revenue_hourly.py` (line 480)

**Code:**
```python
events_stream_merge >> ssp_merge >> syndication_merge >> demand_partner_merge >> gam_merge
```

**Problem:** The five pipeline merges are chained sequentially to prevent concurrent BigQuery writes. All tasks use the default `trigger_rule='all_success'`. When any middle-chain merge is SKIPPED — because the ShortCircuitOperator upstream found no new data — Airflow propagates the SKIPPED state to all downstream tasks in the chain.

Two concrete failure paths:

1. **SSP has no new data (most hours):** `ssp_continue_if_new_data` short-circuits → `ssp_merge` SKIPPED → `syndication_merge` SKIPPED → `demand_partner_merge` SKIPPED → `gam_merge` SKIPPED.
2. **Syndication has no new data:** `syndication_continue_if_new_data` short-circuits → `syndication_merge` SKIPPED → `demand_partner_merge` SKIPPED → `gam_merge` SKIPPED.

In both cases demand_partner and GAM pipelines never reach their merge step, regardless of whether they have data ready to write.

**Failure scenario:**
- GAM is time-gated to 17:00 UTC. On that exact hour, if SSP or syndication found no new data, `syndication_merge` (or `ssp_merge`) is SKIPPED, and `gam_merge` is blocked. GAM actual revenue never lands in the SSOT for that day.
- Demand_partner runs when its `continue_if_new_data` fires. Even if demand_partner data is present, its merge is blocked whenever syndication happens to have nothing new.

**Suggested fix:** Change the trigger rule on the downstream merge tasks in the cross-pipeline chain:
```python
demand_partner_merge = _bq_sql_task(
    ...
    trigger_rule="none_failed_or_skipped",  # run even if upstream was skipped
)
gam_merge = _bq_sql_task(
    ...
    trigger_rule="none_failed_or_skipped",
)
```
Or replace the linear `>>` chain with a dedicated sequencing mechanism (e.g., an `ExternalTaskSensor` or a dummy gate task) that is not affected by skip state propagation.

**Status:** ✅ Fixed — added `trigger_rule="none_failed"` to `syndication_merge`, `demand_partner_merge`, and `gam_merge` in `Model_consolidated_revenue_hourly.py`. SKIPPED upstreams in the cross-pipeline chain no longer block downstream merges.

---

### #2: Daily revenue and article_count multiplied ×24 when exploded to hourly rows

**Task / File:** `syndication_explode_data` / `syndication_explode_data.sql`

**Code:**
```sql
article_count as reconciled_event_count,
d.total_revenue as actual_revenue,
...
from `minute-media-490214`.`minute_media_STG`.`syndication_pull_data` d
cross join unnest(
  generate_timestamp_array(
    timestamp(d.event_date),
    timestamp_add(timestamp(d.event_date), interval 23 hour),
    interval 1 hour
  )
) as event_hour
```

**Problem:** `total_revenue` is a daily total from the source. The CROSS JOIN generates exactly 24 rows per source record (one per UTC hour). Both `actual_revenue` and `reconciled_event_count` are copied as-is to every hourly row without division. Any consumer that aggregates syndication revenue by day from the SSOT will receive `total_revenue × 24`.

**Failure scenario:**
- Source: `organization_id='org_1'`, `partner_name='PartnerA'`, `transaction_date='2024-01-18'`, `total_revenue=100.00`, `article_count=50`
- SSOT after merge: 24 rows, each with `actual_revenue=100.00`, `reconciled_event_count=50`
- `SELECT SUM(actual_revenue) FROM fact_consolidated_revenue_hourly WHERE network='Syndication' AND event_date='2024-01-18'` → returns `2400.00` instead of `100.00`

**Suggested fix:**
```sql
ROUND(d.total_revenue / 24, 6) as actual_revenue,
CAST(ROUND(article_count / 24) AS INT64) as reconciled_event_count,
```
Note: integer division of `article_count` by 24 may not be exact. Consider keeping the full daily `article_count` only on the first hour (e.g., `event_hour = timestamp(d.event_date)`) and NULL or 0 for the remainder, depending on downstream reporting requirements.

**Status:** ✅ Fixed — `syndication_explode_data.sql` updated: `total_revenue / 24` for revenue; integer floor division (`DIV(article_count, 24)`) for event count with the modulo remainder added to the first hour, guaranteeing exact daily total conservation.

---

### #3: check_new_data uses strict > but pull_data uses >= (watermark boundary inconsistency)

**Task / File:** `syndication_check_new_data` / `syndication_check_new_data.sql` and `syndication_pull_data` / `syndication_pull_data.sql`

**Code:**
```sql
-- syndication_check_new_data.sql
where transaction_date > date('{{ ti.xcom_pull(task_ids="syndication_get_increment")[0][0] }}')

-- syndication_pull_data.sql
where transaction_date >= '{{ ti.xcom_pull(task_ids="syndication_get_increment")[0][0] }}'
```

**Problem:** `check_new_data` skips when no date strictly newer than the watermark exists. But `pull_data` uses `>=`, so whenever the pipeline does run, it always re-fetches the watermark date's data (already merged in a prior run). The STG and explode tables are `CREATE OR REPLACE`, so no STG-side duplication occurs. The UPSERT merge handles the re-merged rows correctly (MATCHED → UPDATE). Correctness is maintained, but the inconsistency is confusing and adds avoidable compute.

**Suggested fix:** Make both consistent. Either:
- Change `pull_data` to `> date(watermark)` (pull only genuinely new dates), or
- Accept the overlap as an intentional safety net (document it).

**Status:** ⚠️ Accepted Risk — UPSERT merge is idempotent; the overlap acts as a self-healing safety net at negligible cost for daily-granularity data.

---

### #4: Historical re-delivery of past dates silently ignored

**Task / File:** `syndication_check_new_data` / `syndication_check_new_data.sql`

**Code:**
```sql
where transaction_date > date('{{ ti.xcom_pull(task_ids="syndication_get_increment")[0][0] }}')
```

**Problem:** The data-availability check is strictly forward-looking. If a vendor corrects and re-delivers syndication data for a date that is already below the current watermark (e.g., a backdated revenue correction for two weeks ago), the check will return `has_new_data = false`, the ShortCircuit will fire, and the corrected data will never be pulled or merged.

Unlike the events path (which has a configurable forward window from the watermark), the syndication path has no backward-looking buffer at all.

**Suggested fix:** Add an explicit mechanism for historical corrections: either a configurable lookback window (`transaction_date >= date_sub(watermark, interval N day)` in the pull step), or a separate manual-backfill DAG.

**Status:** ⚠️ Accepted Risk — Low probability for daily syndication data in a POC context. Production deployments should address this.

---

### #5: Partition pruning in merge uses fixed -1 day offset from watermark

**Task / File:** `syndication_merge` / `syndication_merge.sql`

**Code:**
```sql
-- Partition pruning: limit target scan to relevant dates
target.event_date >= date_sub('{{ ti.xcom_pull(task_ids="syndication_get_increment")[0][0] }}' , interval 1 day)
```

**Problem:** The partition filter is derived from the watermark (the maximum `max_syndication_date` already in the SSOT), not from the actual date range of the incoming source data. In the normal case (one new date per run), the source rows have `event_date = watermark` or `event_date = watermark + N`, and the filter `event_date >= watermark - 1 day` correctly covers them.

In a backfill or multi-date delivery scenario:
- Source contains rows for dates from `watermark - 10 days` through `watermark` (old corrections + new data).
- For source rows with `event_date < watermark - 1 day`, the target scan does not include those partitions.
- WHEN MATCHED cannot fire for those old target rows — they are excluded by the partition filter.
- If those target rows already exist in the SSOT (from a previous run), they will NOT be updated. WHEN NOT MATCHED would fire instead and attempt to insert duplicates.

**Evidence:** This is a latent risk. Under normal single-date delivery it does not manifest. Actual impact depends on vendor delivery patterns.

**Suggested fix:** Use a dynamic bound derived from the source data rather than the watermark:
```sql
target.event_date >= (select min(event_date) from `...`.`syndication_explode_data`)
```

**Status:** ⚠️ Accepted Risk — Syndication delivers one date at a time under normal operations; multi-date backfills are not expected in the POC scope.

---

## Validation Queries

Queries used during analysis to verify assumptions and discover issues.

### Query 1: Syndication source — date range and revenue distribution
```sql
SELECT
  transaction_date,
  content_property,
  partner_name,
  article_count,
  total_revenue
FROM `minute-media-490214`.`minute_media_DWH`.`fact_syndication_revenue`
ORDER BY transaction_date DESC
LIMIT 20
```
Used to confirm the shape of source data and verify that `total_revenue` is daily-level (not hourly).

### Query 2: Current syndication watermark — DWH vs source max
```sql
-- DWH watermark
SELECT MAX(max_syndication_date) as dwh_watermark
FROM `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`
WHERE network = 'Syndication';

-- Source max date
SELECT MAX(transaction_date) as source_max
FROM `minute-media-490214`.`minute_media_DWH`.`fact_syndication_revenue`
```
Confirms whether the watermark is up to date and whether there is unprocessed source data.

### Query 3: Revenue overstatement check — SSOT sum vs source total
```sql
-- SSOT daily sum for syndication
SELECT
  event_date,
  organization_id,
  demand_owner,
  SUM(actual_revenue) as ssot_daily_sum,
  COUNT(*) as hour_rows
FROM `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`
WHERE network = 'Syndication'
GROUP BY event_date, organization_id, demand_owner
ORDER BY event_date DESC;

-- Source daily total
SELECT
  transaction_date,
  content_property,
  partner_name,
  total_revenue as source_total
FROM `minute-media-490214`.`minute_media_DWH`.`fact_syndication_revenue`
ORDER BY transaction_date DESC
```
If `ssot_daily_sum = source_total × 24`, issue #2 is confirmed.

### Query 4: Duplicate grain check on syndication rows in SSOT
```sql
SELECT
  event_date,
  event_hour,
  event,
  organization_id,
  demand_owner,
  COUNT(*) as cnt
FROM `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`
WHERE network = 'Syndication'
GROUP BY ALL
HAVING cnt > 1
```
Confirms whether the UPSERT merge correctly handles re-runs without creating duplicate rows.

### Query 5: Hourly row count per syndication date
```sql
SELECT
  event_date,
  COUNT(DISTINCT event_hour) as distinct_hours,
  COUNT(*) as total_rows
FROM `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`
WHERE network = 'Syndication'
GROUP BY event_date
ORDER BY event_date DESC
```
Confirms that exactly 24 hourly rows are created per (date, organization_id, partner_name) combination and that the explode step is generating the expected number of rows.

### Query 6: Cross-pipeline skip risk — demand_partner rows on dates matching syndication dates
```sql
-- Check whether demand_partner actual_revenue was populated on dates
-- where syndication data also arrived (if both pipelines ran successfully)
SELECT
  event_date,
  network,
  COUNTIF(actual_revenue IS NOT NULL) as rows_with_actual_revenue,
  COUNT(*) as total_rows
FROM `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`
WHERE network IN ('Syndication', 'Triplelift', 'IndexExchange', 'Magnite')
GROUP BY event_date, network
ORDER BY event_date DESC, network
```
If demand_partner networks show `rows_with_actual_revenue = 0` on dates where syndication rows exist, it may indicate that the cross-pipeline skip (issue #1) blocked the demand_partner merge on that DAG run.
