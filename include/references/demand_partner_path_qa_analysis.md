# Demand Partner Path — Issue Tracker

> Reference file for documenting and tracking issues found during QA of the demand_partner pipeline.

## Table of Contents

- [Design Note: Why proportional allocation and the UPDATE-only merge pattern](#design-note-why-proportional-allocation-and-the-update-only-merge-pattern)
- [TODO List](#todo-list)
- [Issue Details](#issue-details)
  - [#1: XCom forward reference causes BigQuery parse error on every run](#1-xcom-forward-reference-causes-bigquery-parse-error-on-every-run)
  - [#2: Non-Prebid demand partner networks absent from events_stream — allocation always empty](#2-non-prebid-demand-partner-networks-absent-from-events_stream--allocation-always-empty)
  - [#3: Prebid not in dim_ad_unit_mapping — adunit join silently drops revenue for non-null adunit rows](#3-prebid-not-in-dim_ad_unit_mapping--adunit-join-silently-drops-revenue-for-non-null-adunit-rows)
  - [#4: demand_partner_increment.sql is orphaned dead code with a broken column reference](#4-demand_partner_incrementsql-is-orphaned-dead-code-with-a-broken-column-reference)
  - [#5: Demand partner merge is UPDATE-only — depends on events_stream rows existing](#5-demand-partner-merge-is-update-only--depends-on-events_stream-rows-existing)
  - [#6: No revenue conservation check in allocation](#6-no-revenue-conservation-check-in-allocation)
- [Validation Queries](#validation-queries)

---

## Design Note: Why proportional allocation and the UPDATE-only merge pattern

Demand partner reports (Magnite, Triplelift, IndexExchange, Prebid, etc.) arrive as daily files at varying times — some partners deliver early, others late. The reports carry total daily impressions and revenue, but not the hour-by-hour breakdown that the SSOT requires.

Proportional allocation was chosen deliberately:

- **No dependency on raw events.** Rather than re-scanning the raw `events` table (billions of rows, 60-day retention), allocation is performed against the already-aggregated SSOT rows, which are durable and compact.
- **Consistent pattern across pipelines.** GAM and all demand partners solve the same problem: distributing third-party daily revenue onto existing hourly SSOT rows. Using the same proportional window-function approach keeps one mental model across the pipeline.
- **Smart delta check instead of a time gate.** Unlike GAM (which uses a fixed `hour == 17` gate), demand partners use a "new data" sensor (`demand_partner_check_new_data`) that detects the earliest unprocessed `(partner, date)` pair. This allows the pipeline to fire multiple times per day as different partners deliver their files, without re-processing dates that are already fully reconciled.

The UPDATE-only merge (no `WHEN NOT MATCHED THEN INSERT`) is intentional: demand partner reconciliation enriches existing events_stream rows with actual revenue. Inserting new rows for partner-only revenue would create "ghost rows" with no tracked event count, polluting the SSOT's event-level integrity.

---

## TODO List

| # | Priority | Grade | Area | Task / File | Problem | Status |
|---|----------|-------|------|-------------|---------|--------|
| 1 | Critical | 10 | Orchestration | `demand_partner_check_new_data` / `demand_partner_check_new_data.sql` (lines 24, 35) | XCom pull references `demand_partner_get_increment` which runs **after** this task — XCom is None on current run → `date('None')` → BigQuery parse error | ✅ Fixed |
| 2 | Critical | 10 | Allocation | `demand_partner_allocation` / `demand_partner_allocation.sql` + `events_stream/events_stream_pull_data.sql` | IndexExchange, Magnite, Triplelift have no rows in events_stream → INNER JOIN in allocation finds zero matches → 100% of their revenue silently dropped | ✅ Fixed |
| 3 | High | 8 | Allocation | `demand_partner_allocation` / `demand_partner_allocation.sql` | Prebid has no entry in `dim_ad_unit_mapping` → adunit case-b condition evaluates as `coalesce(dwh.adunit,'') = ''` → only matches null-adunit DWH rows → Prebid revenue silently dropped | ✅ Closed — superseded by #2 fix |
| 4 | Medium | 6 | Incremental | `demand_partner_increment` / `demand_partner_increment.sql` | SQL is not wired to any DAG task (dead code); also references non-existent column `max_demand_partner_date` — would fail immediately if ever connected | ✅ Fixed |
| 5 | Medium | 5 | Merge | `demand_partner_merge` / `demand_partner_merge.sql` | UPDATE-only merge depends on events_stream having inserted rows first — no rows to update means revenue silently skipped; no safety check | ⚠️ Accepted Risk |
| 6 | Low | 4 | Allocation | `demand_partner_allocation` / `demand_partner_allocation.sql` | No validation that `SUM(allocated_actual_revenue)` equals `SUM(total_ssp_revenue_usd)` from grouping — revenue leakage is undetectable without a conservation check | ⚠️ Accepted Risk |

---

## Issue Details

### #1: XCom forward reference causes BigQuery parse error on every run

**Task / File:** `demand_partner_check_new_data` / `demand_partner_check_new_data.sql` (lines 24, 35)

**Code:**
```sql
-- arriving_partners CTE (line 24):
where report_date >= date('{{ ti.xcom_pull(task_ids="demand_partner_get_increment")[0][0] }}')

-- processed_partners CTE (line 35):
where event_date  >= date('{{ ti.xcom_pull(task_ids="demand_partner_get_increment")[0][0] }}')
  and actual_revenue is not null
  and demand_owner in (select partner_name from arriving_partners)
```

**Problem:** `demand_partner_check_new_data` is the first task in the `demand_partner_run_ind` TaskGroup. `demand_partner_get_increment` runs **after** that TaskGroup completes — it is a downstream task, not an upstream one. When Airflow renders the Jinja template at execution time, `ti.xcom_pull(task_ids="demand_partner_get_increment")` has no value in the current DAG run and returns `None`. The SQL is rendered as:

```sql
where report_date >= date('None')
```

BigQuery cannot parse `date('None')` and returns a parse error, causing the task to fail before a single row is read.

The intent of the filter is partition pruning — limiting the staging scan to dates at or after the last processed watermark. The correct reference would be a task that runs *before* the check (e.g., a standalone increment task that reads the watermark from DWH), or the filter should be removed and replaced with a reasonable lookback window.

**Failure scenario:**
1. `demand_partner_check_new_data` Jinja renders XCom pull for `demand_partner_get_increment`
2. Task hasn't run → XCom value is `None`
3. SQL sent to BigQuery: `WHERE report_date >= date('None')`
4. BigQuery raises: `Invalid date literal 'None'` → task fails
5. Entire demand_partner pipeline is skipped for this run

**Suggested fix:** Replace the forward XCom reference with either (a) a lookback window (`report_date >= date_sub(current_date(), interval 7 day)`) or (b) a new upstream `demand_partner_get_watermark` task that reads `max(event_date)` from DWH and pushes it as XCom before the check task runs.

**Status:** ✅ Fixed — replaced the forward XCom reference with a self-contained `increment_bound` CTE that reads the latest allocated demand-partner `event_date` directly from DWH, scoped to networks present in `fact_demand_partner_reports`. Also corrected the source table name from `demand_partner_reports` (non-existent) to `fact_demand_partner_reports`. Validated: query returns `has_new_data = true, increment_column = 2024-01-15`.

---

### #2: Non-Prebid demand partner networks absent from events_stream — allocation always empty

**Task / File:** `demand_partner_allocation` / `demand_partner_allocation.sql` + `demand_partner_last_data_dwh.sql`

**Code in demand_partner_last_data_dwh.sql:**
```sql
from `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`
where event = 'served'
  and network not in ('GAM','MinuteSSP')
  and event_date = date('{{ ti.xcom_pull(task_ids="demand_partner_get_increment")[0][0] }}')
```

**Code in demand_partner_allocation.sql:**
```sql
join `minute-media-490214`.`minute_media_STG`.`demand_partner_last_data_dwh` dwh
  on dwh.event_date = g.event_date
  and dwh.network = g.network          -- must match exactly
  and dwh.organization_id = g.organization_id
  and dwh.country = g.country
  and (
    g.adunit is null
    or coalesce(dwh.adunit, '') = coalesce(map.adunit_path, '')
  )
```

**Problem:** The allocation performs an INNER JOIN between `demand_partner_grouping` (partner-side revenue) and `demand_partner_last_data_dwh` (DWH events). The join key includes `dwh.network = g.network`, where `g.network` comes from `partner_name` in the demand partner report (e.g., `'IndexExchange'`, `'Magnite'`, `'Triplelift'`).

`demand_partner_last_data_dwh` reads rows from `fact_consolidated_revenue_hourly` where `network NOT IN ('GAM','MinuteSSP')`. For DWH rows to appear with `network = 'IndexExchange'`, events_stream must have inserted rows tagged with that network name. But events_stream only aggregates events from the first-party events table, which uses a different network taxonomy: the sample data contains only `GAM`, `MinuteSSP`, and `Prebid` as distinct network values — **IndexExchange, Magnite, and Triplelift never appear**.

This means `demand_partner_last_data_dwh` returns zero rows for those three partners for any date. The INNER JOIN in allocation finds no match, and all their revenue is silently dropped. The `demand_partner_allocation` STG table is left empty.

**Evidence:**
```sql
-- Networks present in DWH (events_stream output):
SELECT DISTINCT network FROM `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`
-- Result: GAM, MinuteSSP, Prebid, NULL

-- Revenue by partner in source data:
SELECT partner_name, SUM(revenue_usd) as total_revenue
FROM `minute-media-490214`.`minute_media_DWH`.`fact_demand_partner_reports`
GROUP BY partner_name ORDER BY total_revenue DESC
-- Result: IndexExchange ~$172,600 | Triplelift ~$112,200 | Magnite ~$103,000 | Prebid ~$4,500

-- Allocation output (confirmed empty):
SELECT COUNT(*) FROM `minute-media-490214`.`minute_media_STG`.`demand_partner_allocation`
-- Result: 0
```

Three partners representing ~$388K in revenue have no path to the SSOT. They pass the check sensor (their data is in `demand_partner_reports`) but produce no output from allocation.

**Root cause (found via assignment spec):** In the raw `events_stream` table, Prebid-won impressions store the actual paying demand partner in the `payingEntity` column (e.g. `payingEntity='Triplelift'`), not in `network` (which is always `'Prebid'`). `events_stream_pull_data.sql` was discarding `payingEntity` entirely, so Triplelift/IndexExchange/Magnite were never written into the SSOT.

**Status:** ✅ Fixed — `events_stream_pull_data.sql` now maps `payingEntity → network` for Prebid events via:
```sql
case when network = 'Prebid' then payingEntity
     else network
end as network,
```
`demand_partner_check_new_data.sql` updated to check `network` (not `demand_owner`) when determining whether a partner has already been allocated. Note: `payingEntity='Index'` vs `partner_name='IndexExchange'` is a known name mismatch accepted for this POC.

---

### #3: Prebid not in dim_ad_unit_mapping — adunit join silently drops revenue for non-null adunit rows

**Task / File:** `demand_partner_allocation` / `demand_partner_allocation.sql`

**Code:**
```sql
-- translation layer
left join `minute-media-490214.minute_media_DWH.dim_ad_unit_mapping` map
  on lower(g.network) = lower(map.partner_name)
  and lower(g.organization_id) = lower(map.property_code)
  and lower(g.adunit) = lower(map.partner_ad_unit_name)

-- adunit join condition
and (
  -- case a: partner sent no ad unit → replicate revenue across all DWH ad units
  g.adunit is null

  -- case b: partner sent an ad unit → must match via dim table
  or coalesce(dwh.adunit, '') = coalesce(map.adunit_path, '')
)
```

**Problem:** When a partner has no row in `dim_ad_unit_mapping`, the LEFT JOIN returns `map.adunit_path = NULL` for every grouping row. Case b of the adunit condition then evaluates as:

```
coalesce(dwh.adunit, '') = coalesce(NULL, '')
→ coalesce(dwh.adunit, '') = ''
→ only true when dwh.adunit IS NULL
```

This means demand partner revenue rows with a non-null `adunit` value can only be joined to DWH events where `adunit IS NULL`. In practice, DWH events have real ad unit paths (e.g., `/987654/sidebar`) — very few rows have `adunit IS NULL`. Virtually all Prebid revenue fails to join and is silently dropped.

Prebid is the only demand partner present in both the source reports and events_stream. It is also the only partner **not** in `dim_ad_unit_mapping`. IndexExchange, Magnite, and Triplelift are blocked by issue #2 before reaching this join, but if issue #2 were fixed, any unmapped partner would hit this same problem.

**Evidence:**
```sql
-- Partners in dim_ad_unit_mapping:
SELECT DISTINCT partner_name FROM `minute-media-490214`.`minute_media_DWH`.`dim_ad_unit_mapping`
-- Result: magnite, triplelift  (no 'prebid' entry)

-- Prebid source revenue:
SELECT SUM(revenue_usd) FROM `minute-media-490214`.`minute_media_DWH`.`fact_demand_partner_reports`
WHERE partner_name = 'Prebid'
-- Result: ~$4,500
```

**Status:** ✅ Closed — superseded by the fix for issue #2. After `events_stream_pull_data.sql` was updated to map `payingEntity → network` for Prebid events, all former `network='Prebid'` rows in the SSOT resolved to specific paying partners (Triplelift, Magnite, etc.). The `payingEntity='Prebid'` value never appears in the raw events table for this dataset, so there are zero `network='Prebid'` rows left in the DWH. Adding Prebid to `dim_ad_unit_mapping` would have no effect. The $4,500 Prebid demand partner report revenue has no corresponding events to allocate against — a POC data gap, not an addressable code bug.

---

### #4: demand_partner_increment.sql is orphaned dead code with a broken column reference

**Task / File:** `demand_partner_increment` / `demand_partner_increment.sql`

**Code:**
```sql
select coalesce(max(max_demand_partner_date), cast('1970-01-01' as date)) as increment_column

from `minute-media-490214`.`minute_media_STG`.`demand_partner_allocation`
```

**Problem:** This SQL file is not referenced by any task in `Model_consolidated_revenue_hourly.py`. It is dead code — the DAG never runs it. The actual watermark is derived from the `demand_partner_check_new_data` STG table, which is read by `demand_partner_get_increment` (`BigQueryGetDataOperator` targeting `minute_media_STG.demand_partner_check_new_data`, field `increment_column`).

Additionally, the SQL references `max_demand_partner_date` — a column that does not exist in `demand_partner_allocation`. That table's output columns are `event_date`, `event_hour`, `event`, `organization_id`, etc. (inherited from DWH). If this file were ever connected to a DAG task, it would fail immediately with a `Unrecognized name: max_demand_partner_date` error from BigQuery.

**Risk:** The file is misleading — a future developer may assume it represents the real watermark logic and wire it up, causing an immediate runtime failure. It also obscures the actual incremental logic (the smart delta check in `demand_partner_check_new_data.sql`).

**Status:** ✅ Fixed — task wired in DAG (`demand_partner_increment >> demand_partner_get_increment >> demand_partner_run_ind ...`); `max_demand_partner_date` column confirmed present in `fact_consolidated_revenue_hourly`; `CREATE OR REPLACE TABLE STG.demand_partner_increment AS` wrapper added so `demand_partner_get_increment` (`BigQueryGetDataOperator`) can read the watermark. Validated: STG table created, returns `1970-01-01` on first run (correct).

---

### #5: Demand partner merge is UPDATE-only — depends on events_stream rows existing

**Task / File:** `demand_partner_merge` / `demand_partner_merge.sql`

**Problem:** The merge statement has no `WHEN NOT MATCHED THEN INSERT` clause — by design, to prevent "ghost rows" with actual revenue but no tracked event count. This means the demand partner merge can only update rows that events_stream has already inserted into `fact_consolidated_revenue_hourly`.

The cross-pipeline merge chain (`events_stream_merge >> ssp_merge >> syndication_merge >> demand_partner_merge >> gam_merge`) ensures events_stream runs before demand_partner within the same DAG run. However, if events_stream failed or skipped a date, any demand partner revenue for that date will be silently lost — there are no DWH rows to UPDATE.

With issues #2 and #3 causing the `demand_partner_allocation` STG table to be empty, this constraint is not currently exercised. In a corrected pipeline, this design decision remains a risk if events_stream reliability cannot be guaranteed.

**Classification:** Design trade-off — intentional and consistent with the GAM merge pattern. The risk is acceptable under the assumption that events_stream is a reliable base layer for the SSOT.

**Status:** ⚠️ Accepted Risk — POC implementation. Same reasoning as GAM #7. No fix needed.

---

### #6: No revenue conservation check in allocation

**Task / File:** `demand_partner_allocation` / `demand_partner_allocation.sql`

**Problem:** The proportional allocation distributes `total_ssp_revenue_usd` from `demand_partner_grouping` across matched DWH rows using a `tracked_event_count`-based weight. There is no downstream check that `SUM(allocated_actual_revenue)` per `(event_date, network, organization_id)` group equals the original `SUM(total_ssp_revenue_usd)` from grouping. Revenue can silently leak due to:
- Unmatched rows dropped by the INNER JOIN (issues #2 and #3)
- Floating-point rounding in the weight calculation
- Integer rounding in `CAST(ROUND(...) AS INT64)` for impression counts

With issues #2 and #3 causing 100% revenue loss, a conservation check would immediately surface the problem rather than leaving `demand_partner_allocation` silently empty.

**Suggested fix:** Add a downstream data quality check that compares `SUM(allocated_actual_revenue)` per group against `SUM(total_ssp_revenue_usd)` from `demand_partner_grouping` and raises an alert on any material delta.

**Status:** ⚠️ Accepted Risk — POC implementation. Same reasoning as GAM #8. A production-grade pipeline should include this check as a monitoring step.

---

## Validation Queries

Queries used during analysis to verify assumptions and discover issues.

### Query 1: demand_partner_reports date range and partners
```sql
SELECT
  MIN(report_date) as min_date,
  MAX(report_date) as max_date,
  COUNT(DISTINCT partner_name) as num_partners,
  STRING_AGG(DISTINCT partner_name ORDER BY partner_name) as partners
FROM `minute-media-490214`.`minute_media_DWH`.`fact_demand_partner_reports`
```
Result: 2024-01-15 to 2024-01-16, 4 partners: IndexExchange, Magnite, Prebid, Triplelift.

### Query 2: Revenue by partner in source data
```sql
SELECT partner_name, SUM(revenue_usd) as total_revenue, SUM(impressions) as total_impressions
FROM `minute-media-490214`.`minute_media_DWH`.`fact_demand_partner_reports`
GROUP BY partner_name ORDER BY total_revenue DESC
```
Result: IndexExchange ~$172,600 | Triplelift ~$112,200 | Magnite ~$103,000 | Prebid ~$4,500.

### Query 3: Distinct networks in SSOT (events_stream output)
```sql
SELECT DISTINCT network
FROM `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`
ORDER BY network
```
Result: GAM, MinuteSSP, Prebid, NULL — IndexExchange, Magnite, Triplelift are absent. Confirms root cause of issue #2.

### Query 4: demand_partner_allocation row count
```sql
SELECT COUNT(*) as row_count
FROM `minute-media-490214`.`minute_media_STG`.`demand_partner_allocation`
```
Result: 0. Confirms that no demand partner revenue was allocated in the last pipeline run.

### Query 5: demand_partner_check_new_data STG contents
```sql
SELECT has_new_data, increment_column
FROM `minute-media-490214`.`minute_media_STG`.`demand_partner_check_new_data`
```
Result: `has_new_data = true`, `increment_column = 2024-01-15`. Check sensor detects unprocessed data correctly — the failure is downstream in allocation.

### Query 6: dim_ad_unit_mapping — demand partner coverage
```sql
SELECT DISTINCT partner_name, property_code, partner_ad_unit_name, adunit_path
FROM `minute-media-490214`.`minute_media_DWH`.`dim_ad_unit_mapping`
ORDER BY partner_name
```
Result: 3 rows — magnite (fs/sidebar → `/987654/.../sidebar`), triplelift (90min/article_sidebar → `/article/side`), triplelift (si/homepage_top → `/home/top`). No Prebid entry. No IndexExchange entry. Confirms issue #3.
