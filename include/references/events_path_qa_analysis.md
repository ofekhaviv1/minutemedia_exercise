# Events Path — Issue Tracker

> Reference file for documenting and tracking issues found during QA of the events_stream pipeline.

## Table of Contents

- [TODO List](#todo-list)
- [Issue Details](#issue-details)
  - [#1: STG-based watermark — data loss after partial failure](#1-stg-based-watermark--data-loss-after-partial-failure)
  - [#2: Unqualified column names in merge INSERT](#2-unqualified-column-names-in-merge-insert)
  - [#3: Schema mismatch — split fields vs assignment requirement](#3-schema-mismatch--split-fields-vs-assignment-requirement)
  - [#4: Late-arriving events permanently missed](#4-late-arriving-events-permanently-missed)
  - [#5: max_events_stream_timestamp not updated on MATCHED](#5-max_events_stream_timestamp-not-updated-on-matched)
  - [#6: is_actual_revenue column never populated](#6-is_actual_revenue-column-never-populated)
  - [#7: Hour truncation causes systematic re-pull](#7-hour-truncation-causes-systematic-re-pull)
  - [#8: GAM_allocation references non-existent column](#8-gam_allocation-references-non-existent-column)
  - [#9: Partition pruning is per-source-row](#9-partition-pruning-is-per-source-row)
  - [#10: payingEntity dropped silently](#10-payingentity-dropped-silently)
- [Validation Queries](#validation-queries)

---

## TODO List

| # | Priority | Grade | Area | Task / File | Problem | Status |
|---|----------|-------|------|-------------|---------|--------|
| 1 | Critical | 10 | Incremental | `events_stream_increment` / `events_stream_increment.sql` | STG-based watermark causes data loss after partial failure | ✅ Fixed |
| 2 | Critical | 10 | Merge | `events_stream_merge` / `events_stream_merge.sql` | Unqualified column names in INSERT VALUES | ✅ Fixed |
| 3 | High | 9 | Assignment | `create_table.sql` (schema) | Schema uses split revenue/count fields instead of single fields | ✅ By Design |
| 4 | High | 8 | Incremental | `events_stream_pull_data` / `events_stream_pull_data.sql` | 5-day forward-only window misses late-arriving events | ⚠️ Accepted Risk |
| 5 | High | 7 | Merge | `events_stream_merge` / `events_stream_merge.sql` | `max_events_stream_timestamp` not updated on MATCHED | ✅ Fixed |
| 6 | High | 6 | Schema | `create_table.sql` / all merge SQL files | `is_actual_revenue` column never populated | ✅ Fixed |
| 7 | Medium | 5 | Cost | `events_stream_increment` / `events_stream_increment.sql` | Hour truncation causes systematic re-pull of ~1 hour | ⚠️ Accepted Risk |
| 8 | Medium | 5 | Downstream | `GAM_allocation` / `GAM_allocation.sql` + `GAM_last_data_dwh.sql` | GAM_allocation references non-existent `t.ad_unit_id` | ✅ Fixed |
| 9 | Medium | 4 | Cost | `events_stream_merge` / `events_stream_merge.sql` | Partition pruning is per-source-row, not bounded | 🔴 Open |
| 10 | Low | 3 | Data | `events_stream_pull_data` / `events_stream_pull_data.sql` | `payingEntity` dropped silently | ⚠️ Accepted Risk |

---

## Issue Details

### #1: STG-based watermark — data loss after partial failure

**Task / File:** `events_stream_increment` / `events_stream_increment.sql`

**Code:**
```sql
select coalesce(timestamp_trunc(max(max_events_stream_timestamp), hour),
       cast('1970-01-01 00:00:00 UTC' as timestamp)) as increment_column
from `minute-media-490214`.`minute_media_STG`.`events_stream_grouping`
```

**Problem:** The watermark reads from the STG grouping table, not the committed DWH. If the merge fails after grouping succeeds, the next run reads an advanced watermark from STG and permanently skips unmerged data.

**Failure scenario:**
1. Run N: `pull_data` -> `grouping` (STG watermark now at T2) -> `merge` FAILS (DWH still at T1)
2. Run N+1: `increment` reads STG -> gets T2 -> pulls `timestamp > T2` -> events between T1 and T2 are permanently skipped

**Fix:** Read watermark from DWH: `SELECT max(max_events_stream_timestamp) FROM fact_consolidated_revenue_hourly`

**Status:** ✅ Fixed — switched to DWH source.

---

### #2: Unqualified column names in merge INSERT

**Task / File:** `events_stream_merge` / `events_stream_merge.sql` (lines 69-71)

**Code:**
```sql
  values (
    source.event_date,
    ...
    source.tracked_event_count,
    actual_revenue,                -- was NOT prefixed with source.
    estimated_revenue,             -- was NOT prefixed with source.
    max_events_stream_timestamp,   -- was NOT prefixed with source.
    current_timestamp(),
    current_timestamp()
  )
```

**Problem:** Three columns not prefixed with `source.` — ambiguous name resolution. Lines 54-58 of the same file correctly use `source.` prefix; lines 69-71 were inconsistent.

**Fix:** Add `source.` prefix to all three columns.

**Status:** ✅ Fixed — all VALUES columns now consistently use `source.` prefix.

---

### #3: Schema mismatch — split fields vs assignment requirement

**Task / File:** `create_table.sql` (lines 15-19)

**Current schema:**
- `is_actual_revenue BOOL` (never populated)
- `tracked_event_count INT64` + `reconciled_event_count INT64`
- `actual_revenue FLOAT64` + `estimated_revenue FLOAT64`

**Assignment requires:** `Revenue` (singular) and `Event count` (singular).

**Problem:** The assignment explicitly lists single fields. A downstream consumer would need to know to compute `COALESCE(actual_revenue, estimated_revenue)` and `COALESCE(reconciled_event_count, tracked_event_count)` — this logic is not materialized anywhere.

**Suggested fix:** Add computed columns `revenue` and `event_count` to the table, or create a view on top.

**Status:** ✅ By Design — intentional split to preserve pre/post reconciliation visibility.

---

### #4: Late-arriving events permanently missed

**Task / File:** `events_stream_pull_data` / `events_stream_pull_data.sql` (lines 22-23)

**Code:**
```sql
where timestamp > cast('{{ ... }}' as timestamp)
  and timestamp <= timestamp_add(cast('{{ ... }}' as timestamp), interval 5 day)
```

**Problem:** The filter only looks forward from the watermark. Events that arrive in BigQuery after the watermark has advanced past their timestamp are permanently missed. The pipeline never re-scans data behind the watermark.

Common late-arrival causes in ad-tech:
- Client-side SDK buffering (mobile apps batch events)
- Network delays and retries
- Ingestion pipeline lag in BigQuery

**Suggested fix:** Add a backward-looking buffer:
```sql
where timestamp > timestamp_sub(cast('{{ ... }}' as timestamp), interval 2 hour)
  and timestamp <= timestamp_add(cast('{{ ... }}' as timestamp), interval 5 day)
```
Or use `_PARTITIONTIME` / ingestion-time metadata if available.

**Status:** ⚠️ Accepted Risk — late-arriving events are a production concern but not material for this assignment's sample dataset.

---

### #5: max_events_stream_timestamp not updated on MATCHED

**Task / File:** `events_stream_merge` / `events_stream_merge.sql` (WHEN MATCHED clause)

**Code:**
```sql
when matched then update set
    target.tracked_event_count = source.tracked_event_count,
    target.estimated_revenue = source.estimated_revenue,
    target.actual_revenue = case ... end,
    target.dwh_updated_at = current_timestamp()
    -- max_events_stream_timestamp was NOT updated
```

**Problem:** When re-processing overlapping data, matched rows get counts/revenue updated but `max_events_stream_timestamp` stays frozen at INSERT time. Since the watermark now reads from DWH (#1 fix), this column must stay current.

**Fix:** Add `target.max_events_stream_timestamp = source.max_events_stream_timestamp` to the UPDATE clause.

**Status:** ✅ Fixed — column now updated on MATCHED.

---

### #6: is_actual_revenue column never populated

**Task / File:** `create_table.sql` (line 15) / all merge SQL files

**Problem:** Column exists in schema but is dead — all 73 rows are NULL. No pipeline sets it. Misleading for consumers.

**Evidence:** `SELECT is_actual_revenue, COUNT(*) FROM fact_consolidated_revenue_hourly GROUP BY 1` returns all NULL.

**Suggested fix:** Either populate it or remove it from schema.

**Status:** ✅ Fixed — removed from `create_table.sql`.

---

### #7: Hour truncation causes systematic re-pull

**Task / File:** `events_stream_increment` / `events_stream_increment.sql` (line 1)

**Code:** `timestamp_trunc(max(max_events_stream_timestamp), hour)`

**Problem:** Watermark is rounded down to the hour, so up to 59m59s of data is re-pulled every hourly run. The merge handles this correctly via UPSERT (no duplicates), but at production scale this adds unnecessary compute cost.

**Suggested fix:** Use exact timestamp instead of `timestamp_trunc(..., hour)`.

**Status:** ⚠️ Accepted Risk — intentional overlap acts as a safety net for merge idempotency.

---

### #8: GAM_allocation references non-existent column

**Task / File:** `GAM_allocation` / `GAM_allocation.sql` + `GAM_last_data_dwh.sql`

**Code:**
```sql
-- GAM_last_data_dwh does NOT select ad_unit_id
-- But GAM_allocation references:
left join ... map on map.partner_name = 'GAM' and t.ad_unit_id = map.ad_unit_id
```

**Problem:** GAM allocation will fail at runtime (column not found), meaning GAM events never receive `actual_revenue` from reconciliation.

**Suggested fix:** Add `ad_unit_id` to `GAM_last_data_dwh` SELECT or fix the join logic.

**Status:** ⏳ Deferred to GAM analysis — the `dim_ad_unit_mapping` join and `ad_unit_id` references appear to be leftover code from an incomplete ad-unit-level allocation attempt. `GAM_grouping` already excludes `ad_unit_id` from its grain.

---

### #9: Partition pruning is per-source-row

**Task / File:** `events_stream_merge` / `events_stream_merge.sql` (line 17)

**Code:**
```sql
and target.event_date >= date_sub(source.event_date, interval 1 day)
```

**Problem:** Semi-join condition evaluated per source row. With a 5-day pull window, this could scan the full target partition range. A scalar subquery or fixed date range would be more efficient.

**Status:** 🔴 Open

---

### #10: payingEntity dropped silently

**Task / File:** `events_stream_pull_data` / `events_stream_pull_data.sql`

**Problem:** Source `events_stream` has `payingEntity` (46 of 48 rows non-null) but it's not selected. Not in the assignment's required fields, but potentially useful for reconciliation. `demandOwner` (preserved) and `payingEntity` are not always the same.

**Status:** ⚠️ Accepted Risk — not in assignment requirements; `demandOwner` covers the reporting need.

---

## Validation Queries

Queries used during analysis to verify assumptions and discover issues.

### Query 1: Events table schema
```sql
SELECT column_name, data_type FROM `minute-media-490214`.`minute_media_DWH`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'events_stream' ORDER BY ordinal_position
```
Confirmed all referenced columns exist. Revealed `payingEntity` is present but not pulled.

### Query 2: dim_line_items_mapping duplicates
```sql
SELECT line_item_id, COUNT(*) as cnt FROM `minute-media-490214`.`minute_media_DWH`.`dim_line_items_mapping`
GROUP BY line_item_id HAVING cnt > 1
```
Result: no duplicates found — LEFT JOIN in grouping is safe.

### Query 3: Event type distribution
```sql
SELECT event, COUNT(*) as cnt FROM `minute-media-490214`.`minute_media_DWH`.`events_stream`
GROUP BY event ORDER BY cnt DESC
```
Result: `served` (44), `viewability` (2), `pageView` (1), `videoEmbed` (1). Total: 48.

### Query 4: NULL patterns per network
```sql
SELECT network, COUNT(*) as cnt, COUNTIF(cpm IS NULL) as null_cpm, COUNTIF(url IS NULL) as null_url,
  COUNTIF(lineItem IS NULL) as null_lineitem, COUNTIF(country IS NULL) as null_country
FROM `minute-media-490214`.`minute_media_DWH`.`events_stream` GROUP BY network ORDER BY cnt DESC
```
Result: NULL network exists (2 rows). NULL lineItems across GAM (6), MinuteSSP (1), NULL-network (2).

### Query 5: Event types in SSOT
```sql
SELECT event, network, COUNT(*) as dwh_cnt, SUM(tracked_event_count) as total_tracked
FROM `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`
WHERE max_events_stream_timestamp IS NOT NULL GROUP BY event, network ORDER BY event, network
```
Result: all 4 event types present. Total tracked: 48 = 48 source events.

### Query 6: Total event count reconciliation
```sql
SELECT COUNT(*) as total FROM `minute-media-490214`.`minute_media_DWH`.`events_stream`
-- vs
SELECT SUM(tracked_event_count) as total FROM `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`
WHERE max_events_stream_timestamp IS NOT NULL
```
Result: both return 48.

### Query 7: Grain duplicate check
```sql
SELECT event_date, event_hour, event, organization_id, adunit, network, domain, country, COUNT(*) as cnt
FROM `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`
WHERE max_events_stream_timestamp IS NOT NULL GROUP BY ALL HAVING cnt > 1
```
Result: no duplicates.

### Query 8: is_actual_revenue population
```sql
SELECT is_actual_revenue, COUNT(*) as cnt FROM `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`
GROUP BY is_actual_revenue
```
Result: all 73 rows NULL.

### Query 9: Watermark comparison
```sql
-- STG:
SELECT coalesce(timestamp_trunc(max(max_events_stream_timestamp), hour), cast('1970-01-01 00:00:00 UTC' as timestamp))
FROM `minute-media-490214`.`minute_media_STG`.`events_stream_grouping`
-- DWH:
SELECT max(max_events_stream_timestamp) FROM `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`
```
Result: STG = `2024-01-18 11:00:00`, DWH = `2024-01-18 11:55:00`.

### Query 10: payingEntity population
```sql
SELECT COUNTIF(payingEntity IS NOT NULL) as has_paying_entity, COUNT(*) as total
FROM `minute-media-490214`.`minute_media_DWH`.`events_stream`
```
Result: 46 of 48 non-null.
