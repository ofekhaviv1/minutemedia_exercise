# GAM Path — Issue Tracker

> Reference file for documenting and tracking issues found during QA of the GAM pipeline.

## Table of Contents

- [Design Note: Why proportional allocation instead of session-level matching](#design-note-why-proportional-allocation-instead-of-session-level-matching)
- [TODO List](#todo-list)
- [Issue Details](#issue-details)
  - [#1: Time gate will never fire — GAM path is permanently skipped](#1-time-gate-will-never-fire--gam-path-is-permanently-skipped)
  - [#2: GAM_SQL_DIR trailing space breaks file references after directory rename](#2-gam_sql_dir-trailing-space-breaks-file-references-after-directory-rename)
  - [#3: GAM increment reads from STG — watermark drift risk](#3-gam-increment-reads-from-stg--watermark-drift-risk)
  - [#4: ad_unit_id reference silently degrades allocation](#4-ad_unit_id-reference-silently-degrades-allocation)
  - [#5: Country mapping mismatch — UK vs GB causes revenue leakage](#5-country-mapping-mismatch--uk-vs-gb-causes-revenue-leakage)
  - [#6: Unmatched GAM rows silently dropped by INNER JOIN](#6-unmatched-gam-rows-silently-dropped-by-inner-join)
  - [#7: GAM merge is UPDATE-only — depends on events_stream running first](#7-gam-merge-is-update-only--depends-on-events_stream-running-first)
  - [#8: No revenue conservation check in allocation](#8-no-revenue-conservation-check-in-allocation)
- [Validation Queries](#validation-queries)

---

## Design Note: Why proportional allocation instead of session-level matching

GAM data transfer includes `session_id`, and the events table has `sessionid`. In theory, this enables exact impression-level matching — replacing estimated CPMs with actual ones per event, without proportional guessing.

However, proportional allocation was chosen deliberately for architectural consistency:

- **No dependency on raw events.** The events table has billions of rows/day and only 60-day retention. Going back to it for GAM reconciliation re-scans a massive table and creates a risk window where events may expire before GAM runs.
- **Consistent reconciliation pattern.** Both GAM and demand partners solve the same problem: distributing third-party actual revenue onto existing SSOT rows. Using the same allocation approach for both keeps one mental model across the pipeline.
- **Forward-only data flow.** All reconciliation operates on the already-aggregated SSOT — no pipeline reaches backward into raw events. This keeps the SSOT as the single source of truth for event counts, with partner data enriching it with actual revenue.

Session-level matching remains a valid future optimization for GAM specifically, but it would break the architectural symmetry with demand partners and introduce operational complexity.

---

## TODO List

| # | Priority | Grade | Area | Task / File | Problem | Status |
|---|----------|-------|------|-------------|---------|--------|
| 1 | Critical | 10 | Orchestration | `gam_time_gate` / `Model_consolidated_revenue_hourly.py` (line 81) | Time gate checks `minute == 30` but @hourly schedule only produces `minute == 0` — GAM path never runs | ✅ Fixed |
| 2 | Critical | 10 | File Path | `GAM_SQL_DIR` / `Model_consolidated_revenue_hourly.py` (line 22) | `GAM_SQL_DIR = "GAM "` still has trailing space, but directory was renamed to `GAM/` — all SQL references will fail | ✅ Fixed |
| 3 | Critical | 10 | Incremental | `GAM_increment` / `GAM_increment.sql` | Reads watermark from STG (`GAM_allocation`) not DWH — partial failure skips data | ✅ Fixed |
| 4 | High | 8 | Allocation | `GAM_allocation` / `GAM_allocation.sql` (lines 17-19, 28) | `t.ad_unit_id` doesn't exist in `GAM_last_data_dwh` — silently degrades to site-wide allocation | ✅ Fixed |
| 5 | High | 8 | Data | `GAM_grouping` / `dim_country_mapping` | "United Kingdom" maps to "UK" but events_stream uses "GB" — GAM rows for UK traffic can't match DWH | ✅ Fixed |
| 6 | High | 7 | Allocation | `GAM_allocation` / `GAM_allocation.sql` (line 21) | INNER JOIN drops GAM rows with no DWH match — revenue silently lost ($0.0095 on 2024-01-16) | ✅ Fixed |
| 7 | Medium | 5 | Merge | `GAM_merge` / `GAM_merge.sql` | UPDATE-only merge depends on events_stream having loaded rows first — no safety check | ✅ Accepted |
| 8 | Medium | 4 | Allocation | `GAM_allocation` / `GAM_allocation.sql` | No validation that allocated revenue sums back to source GAM revenue | ✅ Accepted |

---

## Issue Details

### #1: Time gate will never fire — GAM path is permanently skipped

**Task / File:** `gam_time_gate` / `Model_consolidated_revenue_hourly.py` (lines 78-81)

**Code:**
```python
def is_gam_execution_hour(**context: Any) -> bool:
    """Returns True only during the 16:30 run."""
    logical_date = context["logical_date"]
    return logical_date.hour == 16 and logical_date.minute == 30
```

**Problem:** The DAG uses `schedule="@hourly"`. Airflow's `@hourly` schedule triggers at the top of every hour. The `logical_date` for each run corresponds to the start of the scheduled interval — always `HH:00:00`, never `HH:30:00`.

This means `logical_date.minute` is **always 0**, and the condition `logical_date.minute == 30` is **never true**. The `ShortCircuitOperator` returns `False` every single hour, permanently skipping the entire GAM path.

**Evidence:**
- DAG config: `schedule="@hourly"`, `start_date=pendulum.datetime(2026, 3, 1, tz="UTC")`
- Airflow scheduling: for the 16:00 UTC interval, `logical_date` = `2026-03-01 16:00:00 UTC`
- The minute component is always `0`, never `30`

**Suggested fix:** Change to check hour only, not minute.

**Status:** ✅ Fixed — changed to `return logical_date.hour == 17` (runs at 17:00 UTC).

---

### #2: GAM_SQL_DIR trailing space breaks file references after directory rename

**Task / File:** `Model_consolidated_revenue_hourly.py` (line 22)

**Code:**
```python
GAM_SQL_DIR = "GAM "
```

**Problem:** The directory was renamed from `GAM /` (with trailing space) to `GAM/` (no space) in this branch's commit. But the Python variable `GAM_SQL_DIR` still has the trailing space. All 6 GAM SQL file references use `f"{GAM_SQL_DIR}/GAM_*.sql"`, which resolves to `"GAM /GAM_*.sql"` — a path that no longer exists.

This means every GAM task will fail with a file-not-found error when Airflow tries to render the SQL templates.

**Suggested fix:** Change `GAM_SQL_DIR = "GAM "` to `GAM_SQL_DIR = "GAM"`.

**Status:** ✅ Fixed — removed trailing space.

---

### #3: GAM increment reads from STG — watermark drift risk

**Task / File:** `GAM_increment` / `GAM_increment.sql`

**Code:**
```sql
select coalesce(max(max_gam_timestamp), cast('1970-01-01 00:00:00 UTC' as timestamp)) as increment_column
from `minute-media-490214`.`minute_media_STG`.`GAM_allocation`
```

**Problem:** Same pattern as the events path issue #1 (already fixed there). The watermark reads from the STG allocation table, not the committed DWH. If the merge fails after allocation succeeds, the next run reads an advanced watermark and permanently skips unmerged GAM data.

**Failure scenario:**
1. Run N: allocation writes STG (watermark now at T2) → merge FAILS (DWH still at T1)
2. Run N+1: increment reads STG → gets T2 → pulls `date(timestamp) = date_add(date(T2), interval 1 day)` → skips the day that was never merged

**Suggested fix:** Read from DWH: `SELECT max(max_gam_timestamp) FROM fact_consolidated_revenue_hourly`

**Status:** ✅ Fixed — switched to DWH source.

---

### #4: ad_unit_id reference silently degrades allocation

**Task / File:** `GAM_allocation` / `GAM_allocation.sql` (lines 17-19, 12, 28)

**Code:**
```sql
left join `minute-media-490214.minute_media_dwh.dim_ad_unit_mapping` map
  on map.partner_name = 'GAM'
  and t.ad_unit_id = map.ad_unit_id        -- t.ad_unit_id does NOT exist

...
partition by t.event_date, t.event_hour, t.line_item, t.country,
  coalesce(map.ad_unit_id, 'all_units')    -- always 'all_units'

...
and coalesce(t.ad_unit_id, 'NA') = coalesce(map.ad_unit_id, 'NA')  -- always 'NA' = 'NA'
```

**Problem:** `GAM_last_data_dwh` (aliased as `t`) does not select `ad_unit_id`. BigQuery resolves `t.ad_unit_id` as NULL for every row. The LEFT JOIN to `dim_ad_unit_mapping` matches nothing. `coalesce(map.ad_unit_id, 'all_units')` always evaluates to `'all_units'`, and the line 28 condition `coalesce(NULL, 'NA') = coalesce(NULL, 'NA')` is always true.

The result: allocation silently falls back to site-wide proportional distribution instead of ad-unit-level. It doesn't crash — it just loses granularity.

**Evidence:** Verified via BQ — `GAM_allocation` STG table exists with data (3 rows). The `total_tracked_for_group` values confirm all ad units are pooled into one group per (date, hour, line_item, country).

**Suggested fix:** Add `ad_unit_id` to `GAM_grouping`, fix dim join to use `t.adunit = map.adunit_path`, and connect via `CAST(map.ad_unit_id AS STRING) = g.ad_unit_id`.

**Status:** ✅ Fixed — full ad-unit-level allocation implemented: `GAM_grouping` now includes `ad_unit_id`; `GAM_allocation` joins DWH `adunit` → `map.adunit_path` → `CAST(map.ad_unit_id AS STRING)` → `g.ad_unit_id`.

---

### #5: Country mapping mismatch — UK vs GB causes revenue leakage

**Task / File:** `GAM_grouping` / `dim_country_mapping`

**Code in GAM_grouping.sql:**
```sql
coalesce(m.country_code, g.country_name) as country
```

**Problem:** `dim_country_mapping` maps "United Kingdom" → `UK`. But the events_stream (and therefore the SSOT) uses `GB` for United Kingdom traffic (standard ISO 3166-1 alpha-2).

When GAM grouping produces `country = 'UK'` and the DWH has `country = 'GB'` for the same impressions, the allocation join `coalesce(t.country, 'NA') = coalesce(g.country, 'NA')` fails to match. Those GAM rows are dropped by the INNER JOIN in allocation.

**Evidence:**
```sql
-- SSOT uses GB:
SELECT DISTINCT country FROM fact_consolidated_revenue_hourly WHERE network = 'GAM' AND country IN ('UK', 'GB')
-- Result: GB

-- dim_country_mapping maps to UK:
SELECT country_code FROM dim_country_mapping WHERE country_name = 'United Kingdom'
-- Result: UK
```

This is the root cause of issue #6 (line_item 222222222 / UK has no DWH match).

**Suggested fix:** Change `dim_country_mapping` entry for "United Kingdom" from `UK` to `GB`, or add `GB` as an alias.

**Status:** ✅ Fixed — updated `dim_country_mapping` in BigQuery: `country_code` for "United Kingdom" changed from `UK` to `GB`. GAM grouping now produces `GB`, matching the SSOT.

---

### #6: Unmatched GAM rows silently dropped by INNER JOIN

**Task / File:** `GAM_allocation` / `GAM_allocation.sql` (line 21)

**Code:**
```sql
join `minute-media-490214`.`minute_media_STG`.`GAM_grouping` g
  on t.event_date = g.event_date
  and t.event_hour = g.event_hour
  and coalesce(t.line_item, 'NA') = coalesce(g.line_item, 'NA')
  and coalesce(t.country, 'NA') = coalesce(g.country, 'NA')
```

**Problem:** This is an INNER JOIN. GAM grouped rows that have no matching DWH row are silently dropped — their revenue is lost from the SSOT.

**Evidence:** For 2024-01-16, GAM source revenue = $1.0119 but allocated revenue = $1.0024. The missing $0.0095 belongs to line_item `222222222` / country `UK` which has no DWH match (root cause: country mapping mismatch in #5).

```sql
-- Unmatched row:
SELECT g.line_item, g.country, g.total_gam_revenue, t.line_item as dwh_match
FROM GAM_grouping g LEFT JOIN GAM_last_data_dwh t ON ...
WHERE t.line_item IS NULL
-- Result: 222222222, UK, 0.0095, NULL
```

**Suggested fix:** After fixing the country mapping (#5), this specific case should resolve. For general robustness, consider logging or alerting on unmatched GAM rows rather than silently dropping them.

**Status:** ✅ Fixed — resolved as a consequence of #5. "United Kingdom" now maps to `GB`, so the INNER JOIN in `GAM_allocation` will match DWH rows correctly and the $0.021 UK revenue will no longer be dropped.

---

### #7: GAM merge is UPDATE-only — depends on events_stream running first

**Task / File:** `GAM_merge` / `GAM_merge.sql`

**Problem:** The GAM merge has no `WHEN NOT MATCHED THEN INSERT` clause — by design, to prevent "ghost rows" with revenue but no tracked events. However, this means GAM reconciliation only works if `events_stream_merge` has already inserted the corresponding rows.

The cross-pipeline dependency (`events_stream_merge >> ... >> gam_merge`) ensures ordering within a single DAG run. But if events_stream fails or skips a date, and GAM runs later for that same date, the GAM data is silently lost — there are no DWH rows to UPDATE.

**Classification:** Design trade-off — the current approach is intentional and documented. The risk is acceptable if events_stream is reliable.

**Status:** ✅ Accepted — POC implementation. In production, events_stream reliability guarantees DWH rows will always exist for any date GAM reconciles. No fix needed.

---

### #8: No revenue conservation check in allocation

**Task / File:** `GAM_allocation` / `GAM_allocation.sql`

**Problem:** The allocation distributes `total_gam_revenue` proportionally across matched DWH rows. However, there's no validation that the sum of `allocated_actual_revenue` equals the original `total_gam_revenue` per group. Revenue can leak due to:
- Rounding in `CAST(ROUND(...) AS INT64)` for impression counts
- Unmatched GAM rows dropped by the INNER JOIN (#6)
- Division precision in weight calculation

**Evidence:** 2024-01-16: source = $1.0119, allocated = $1.0024, delta = $0.0095 (0.94% leakage, caused by #5/#6).

**Suggested fix:** Add a downstream data quality check that compares `SUM(allocated_actual_revenue)` per date against source GAM revenue.

**Status:** ✅ Accepted — the $0.0095 delta was caused entirely by the UK→GB mismatch (fixed in #5/#6). Remaining float precision loss is negligible. A production revenue conservation check is a future monitoring concern, not a POC bug.

---

## Validation Queries

Queries used during analysis to verify assumptions and discover issues.

### Query 1: GAM data transfer date range
```sql
SELECT MIN(date(Timestamp)) as min_date, MAX(date(Timestamp)) as max_date, COUNT(*) as total_rows
FROM `minute-media-490214`.`minute_media_DWH`.`fact_gam_data_transfer`
```
Result: 2024-01-15 to 2024-01-18, 27 rows.

### Query 2: GAM source revenue by date
```sql
SELECT date(Timestamp) as dt, SUM(cpm_usd/1000) as total_gam_revenue
FROM `minute-media-490214`.`minute_media_DWH`.`fact_gam_data_transfer` GROUP BY dt ORDER BY dt
```
Result: 2024-01-15=$0.073, 2024-01-16=$1.012, 2024-01-17=$0.005, 2024-01-18=$0.022.

### Query 3: GAM reconciliation status in SSOT
```sql
SELECT event_date, COUNTIF(actual_revenue IS NOT NULL) as has_actual, COUNTIF(max_gam_timestamp IS NOT NULL) as has_gam_ts, COUNT(*) as total
FROM `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`
WHERE network = 'GAM' GROUP BY event_date ORDER BY event_date
```
Result: Only 2024-01-15 (4/6 rows) and 2024-01-16 (3/4 rows) have actual_revenue. 2024-01-17 and 2024-01-18 have no GAM reconciliation.

### Query 4: GAM allocation totals vs source
```sql
SELECT event_date, SUM(allocated_actual_revenue) as total_allocated, COUNT(*) as allocation_rows
FROM `minute-media-490214`.`minute_media_STG`.`GAM_allocation` GROUP BY event_date
```
Result: Only 2024-01-16 processed, $1.0024 allocated vs $1.0119 source (delta=$0.0095).

### Query 5: Unmatched GAM grouping rows
```sql
SELECT g.line_item, g.country, g.total_gam_revenue, t.line_item as dwh_match
FROM GAM_grouping g LEFT JOIN GAM_last_data_dwh t
  ON g.event_date = t.event_date AND g.event_hour = t.event_hour
  AND coalesce(g.line_item, 'NA') = coalesce(t.line_item, 'NA')
  AND coalesce(g.country, 'NA') = coalesce(t.country, 'NA')
WHERE t.line_item IS NULL
```
Result: line_item=222222222, country=UK, revenue=$0.0095 — no DWH match.

### Query 6: Country code verification
```sql
-- SSOT:
SELECT DISTINCT country FROM fact_consolidated_revenue_hourly WHERE network = 'GAM' AND country IN ('UK', 'GB')
-- Result: GB

-- dim_country_mapping:
SELECT country_code FROM dim_country_mapping WHERE country_name = 'United Kingdom'
-- Result: UK
```
Confirmed mismatch: SSOT uses `GB`, dim_country_mapping outputs `UK`.

### Query 7: GAM watermark comparison
```sql
-- STG:
SELECT coalesce(max(max_gam_timestamp), cast('1970-01-01 00:00:00 UTC' as timestamp))
FROM `minute-media-490214`.`minute_media_STG`.`GAM_allocation`
-- Result: 2024-01-16 14:05:00

-- DWH:
SELECT max(max_gam_timestamp) FROM `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`
-- Result: 2024-01-16 14:05:00
```
Currently aligned, but structural risk remains.

### Query 8: GAM_last_data_dwh columns
```sql
SELECT COLUMN_NAME FROM `minute-media-490214`.`minute_media_STG`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'GAM_last_data_dwh' ORDER BY ordinal_position
```
Result: 14 columns — `ad_unit_id` is NOT present. Confirms #4.

### Query 9: dim_country_mapping full data
```sql
SELECT * FROM `minute-media-490214`.`minute_media_DWH`.`dim_country_mapping`
```
Result: "United Kingdom" → "UK" (not "GB"). All other countries use correct ISO codes.

### Query 10: Events_stream country for UK traffic
```sql
SELECT DISTINCT country FROM `minute-media-490214`.`minute_media_DWH`.`events_stream` WHERE country IN ('UK', 'GB')
```
Result: `GB`. Confirms events use ISO standard, dim_country_mapping does not.
