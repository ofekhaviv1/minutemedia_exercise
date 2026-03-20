# SSP Path — Issue Tracker

> Reference file for documenting and tracking issues found during QA of the SSP pipeline.

## Table of Contents

- [TODO List](#todo-list)
- [Issue Details](#issue-details)
  - [#1: 24x revenue and impression overcounting in explode](#1-24x-revenue-and-impression-overcounting-in-explode)
  - [#2: Stale UK country codes in DWH from old explode code](#2-stale-uk-country-codes-in-dwh-from-old-explode-code)
  - [#3: Check vs pull filter mismatch — strict > vs inclusive >=](#3-check-vs-pull-filter-mismatch--strict--vs-inclusive-)
  - [#4: Short-circuit false positive — own_site data triggers check but pull produces nothing](#4-short-circuit-false-positive--own_site-data-triggers-check-but-pull-produces-nothing)
- [Validation Queries](#validation-queries)

---

## TODO List

| # | Priority | Grade | Area | Task / File | Problem | Status |
|---|----------|-------|------|-------------|---------|--------|
| 1 | Critical | 10 | Explode | `ssp_explode_data` / `ssp_explode_data.sql` (lines 52-53) | Daily impressions and revenue not divided by 24 — each hourly row gets the full daily amount, causing 24x overcounting | ✅ Fixed |
| 2 | High | 7 | Data | DWH `fact_consolidated_revenue_hourly` | Old SSP rows in DWH still have `country = 'UK'` (48 rows) — code was fixed but data not backfilled | ✅ Fixed |
| 3 | Medium | 5 | Orchestration | `ssp_check_new_data` vs `ssp_pull_data` | Check uses `report_date >` (strict) but pull uses `report_date >=` (inclusive) — can block re-processing of current increment date | ⚠️ Accepted Risk |
| 4 | Low | 3 | Orchestration | `ssp_check_new_data` / `ssp_check_new_data.sql` | Check scans all site_types but pull filters to external only — own_site-only arrivals trigger check but produce empty pull | ✅ Fixed |

### Items verified as correct

| Area | Verdict |
|------|---------|
| **Increment source** | ✅ Already reads from DWH (`fact_consolidated_revenue_hourly`) — safe against partial failure |
| **Country mapping** | ✅ Code fixed to join on `country_code_alpha3` — GBR→GB works correctly |
| **Merge idempotency** | ✅ UPSERT — reruns overwrite same values on MATCHED, insert on NOT MATCHED. No duplicates (verified via BQ) |
| **Merge key correctness** | ✅ 13-dimension key with COALESCE null-safety. Effectively `(date, hour, org, media_type, country)` for SSP rows |
| **Overwrite safety** | ✅ SSP only writes `reconciled_event_count`, `actual_revenue`, `max_ssp_date`. Does not touch events_stream fields |
| **Network isolation** | ✅ Merge key includes `network = 'MinuteSSP'`. No downstream path targets MinuteSSP external rows |
| **No events_stream overlap** | ✅ `site_type IN ('external', 'ext_player')` filter prevents double-counting with events_stream MinuteSSP rows (verified via BQ) |
| **Upstream dependency** | ✅ `events_stream_merge >> ssp_merge` guarantees events rows exist first. SSP inserts NEW rows, not enriching existing ones |
| **Partial-failure safety** | ✅ Increment from DWH, check from source table, STG is CREATE OR REPLACE, merge is idempotent |
| **Late-arriving data** | ✅ SSP reports are daily aggregates — late arrivals have `report_date > increment` and are detected by check. No backward-looking window concern |
| **Timezone** | ✅ DAG uses UTC, SSP data uses DATE (no timezone component). No mismatch |

---

## Issue Details

### #1: 24x revenue and impression overcounting in explode

**Task / File:** `ssp_explode_data` / `ssp_explode_data.sql` (lines 52-53)

**Code:**
```sql
impressions as reconciled_event_count,
revenue_usd as actual_revenue,
```

**Problem:** The explode step creates 24 hourly rows per source row via `CROSS JOIN UNNEST(generate_timestamp_array(...))`. However, `impressions` and `revenue_usd` are used **as-is** without dividing by 24. Each hourly row receives the **full daily amount**, resulting in 24x overcounting of both impressions and revenue.

**Evidence:**
```
Source: external_pub1, 2024-01-15
  impressions = 50,000    revenue_usd = $500,000

Explode output: 24 hourly rows, EACH with:
  reconciled_event_count = 50,000    actual_revenue = $500,000

Total in SSOT: 1,200,000 impressions / $12,000,000 revenue (24x)
```

Verified across all external publishers:
| Publisher | Source imps | Explode total | Ratio |
|---|---|---|---|
| external_pub1 | 102,000 | 2,448,000 | 24x |
| external_pub2 | 200,000 | 4,800,000 | 24x |
| publisher_x | 120,000 | 2,880,000 | 24x |
| external_pub3 | 500 | 12,000 | 24x |

**Suggested fix:** Divide by 24 in the explode step:
```sql
cast(impressions / 24 as int64) as reconciled_event_count,
revenue_usd / 24 as actual_revenue,
```

**Status:** ✅ Fixed — added `/ 24` to both impressions and revenue in the explode step. Daily values are now evenly spread across 24 hours, keeping the fact table additive.

---

### #2: Stale UK country codes in DWH from old explode code

**Task / File:** DWH `fact_consolidated_revenue_hourly` — SSP external rows

**Problem:** The explode code was updated (in a recent dev pull) to correctly map 3-letter country codes via `dim_country_mapping.country_code_alpha3` — `GBR` now correctly maps to `GB`. However, 48 existing DWH rows from earlier runs still have `country = 'UK'` (the old incorrect mapping).

**Evidence:**
```sql
-- Current STG explode output (correct):
SELECT DISTINCT country FROM ssp_explode_data WHERE organization_id = 'external_pub1'
-- Result: GB

-- DWH (stale):
SELECT country, COUNT(*) FROM fact_consolidated_revenue_hourly
WHERE network = 'MinuteSSP' AND tracked_event_count IS NULL AND country IN ('UK', 'GB')
GROUP BY country
-- Result: UK = 48 rows
```

**Suggested fix:** Backfill UPDATE to correct existing rows:
```sql
UPDATE fact_consolidated_revenue_hourly
SET country = 'GB', dwh_updated_at = CURRENT_TIMESTAMP()
WHERE network = 'MinuteSSP' AND tracked_event_count IS NULL AND country = 'UK'
```

Or re-run the SSP pipeline after fixing the increment to re-process those dates.

**Status:** ✅ Fixed — backfilled via BQ UPDATE: 48 rows corrected from `UK` to `GB`.

---

### #3: Check vs pull filter mismatch — strict > vs inclusive >=

**Task / File:** `ssp_check_new_data` / `ssp_check_new_data.sql` vs `ssp_pull_data` / `ssp_pull_data.sql`

**Code:**
```sql
-- ssp_check_new_data.sql:
WHERE report_date > date('{{ ... }}')    -- strict greater than

-- ssp_pull_data.sql:
WHERE report_date >= '{{ ... }}'         -- inclusive greater than or equal
```

**Problem:** With increment = `2024-01-16`:
- Check: `report_date > 2024-01-16` → 0 rows → returns FALSE → short-circuit skips
- Pull: `report_date >= 2024-01-16` → 1 row → would have data to process

This means if the only available data is for the current increment date, the check blocks it. In practice, this is acceptable because the increment only advances after successful merge — the data for the increment date was already processed in the previous run.

**Status:** ⚠️ Accepted Risk — the strict check prevents wasteful re-processing of already-merged data. The inclusive pull ensures idempotency on manual reruns (when short-circuit is bypassed).

---

### #4: Short-circuit false positive — own_site data triggers check but pull produces nothing

**Task / File:** `ssp_check_new_data` / `ssp_check_new_data.sql`

**Code:**
```sql
-- Check scans ALL site_types:
FROM fact_ssp_report WHERE report_date > ...

-- Pull filters to external only:
WHERE ... AND site_type IN ('external', 'ext_player')
```

**Problem:** If new SSP data arrives only for `own_site` or `own_player` (no new external data), the check returns TRUE but the pull step produces an empty staging table. The explode and merge then execute as no-ops.

**Classification:** Harmless — no incorrect data, just wasted compute. Adding `site_type IN ('external', 'ext_player')` to the check would prevent this, but it's a minor optimization.

**Status:** ✅ Fixed — added `AND site_type IN ('external', 'ext_player')` to align check with pull filter.

---

## Validation Queries

### Query 1: SSP source data
```sql
SELECT * FROM `minute-media-490214`.`minute_media_DWH`.`fact_ssp_report` ORDER BY report_date, publisher_id
```
Result: 9 rows across 2024-01-15 and 2024-01-16. External/ext_player: 5 rows; own_site/own_player: 4 rows.

### Query 2: SSP increment value
```sql
SELECT coalesce(max(max_ssp_date), date('1970-01-01')) as increment_column
FROM `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`
WHERE network = 'MinuteSSP' AND tracked_event_count IS NULL
```
Result: `2024-01-16`.

### Query 3: Explode overcounting verification
```sql
-- Source totals:
SELECT publisher_id, SUM(impressions) as src_imps, ROUND(SUM(revenue_usd), 2) as src_rev
FROM fact_ssp_report WHERE site_type IN ('external', 'ext_player') GROUP BY publisher_id

-- Explode totals:
SELECT organization_id, SUM(reconciled_event_count) as exp_imps, ROUND(SUM(actual_revenue), 2) as exp_rev
FROM ssp_explode_data GROUP BY organization_id
```
Result: Every publisher shows 24x overcounting in explode vs source.

### Query 4: Stale UK rows in DWH
```sql
SELECT country, COUNT(*) as cnt, SUM(actual_revenue) as rev
FROM fact_consolidated_revenue_hourly
WHERE network = 'MinuteSSP' AND tracked_event_count IS NULL AND country IN ('UK', 'GB')
GROUP BY country
```
Result: UK = 48 rows (stale). No GB rows yet (code fixed but not re-run).

### Query 5: Current explode country output
```sql
SELECT country, organization_id, COUNT(*) as cnt FROM ssp_explode_data GROUP BY ALL
```
Result: GB (48), CA (24), FR (24), MX (24). GBR→GB mapping now works correctly.

### Query 6: No duplicate rows at merge grain
```sql
SELECT event_date, event_hour, organization_id, media_type, country, COUNT(*) as cnt
FROM fact_consolidated_revenue_hourly
WHERE network = 'MinuteSSP' AND tracked_event_count IS NULL
GROUP BY ALL HAVING cnt > 1
```
Result: no duplicates.

### Query 7: No overlap between events_stream and SSP external rows
```sql
SELECT t1.organization_id, t1.event_date, t1.country, t1.media_type
FROM fact_consolidated_revenue_hourly t1
WHERE t1.network = 'MinuteSSP' AND t1.tracked_event_count IS NOT NULL
INTERSECT DISTINCT
SELECT t2.organization_id, t2.event_date, t2.country, t2.media_type
FROM fact_consolidated_revenue_hourly t2
WHERE t2.network = 'MinuteSSP' AND t2.tracked_event_count IS NULL
```
Result: no overlap. Events_stream and SSP external rows are fully disjoint.

### Query 8: Check vs pull filter comparison
```sql
SELECT 'check_strict_gt' as filter, count(*) as rows_found
FROM fact_ssp_report WHERE report_date > date('2024-01-16')
UNION ALL
SELECT 'pull_gte', count(*)
FROM fact_ssp_report WHERE report_date >= date('2024-01-16') AND site_type IN ('external', 'ext_player')
```
Result: check = 0 rows, pull = 1 row. Demonstrates the > vs >= mismatch.

### Query 9: MinuteSSP rows by origin
```sql
SELECT event_date,
  CASE WHEN tracked_event_count IS NULL THEN 'external' ELSE 'events_stream' END as origin,
  COUNT(*) as row_cnt, SUM(actual_revenue) as total_rev
FROM fact_consolidated_revenue_hourly WHERE network = 'MinuteSSP'
GROUP BY event_date, origin ORDER BY event_date, origin
```
Result: 2024-01-15 has 2 events_stream + 96 external; 2024-01-16 has 48 external; 2024-01-17 has 1 events_stream.
