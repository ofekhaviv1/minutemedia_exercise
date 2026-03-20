/*
  task: demand_partner_check_new_data
  description:
  this sensor implements a "smart delta check". instead of just checking if new data exists,
  it identifies the earliest date that still has at least one unprocessed demand partner.
  a partner is considered "unprocessed" for a given date if it appears in the staging table
  but has no entry with actual_revenue is not null in the dwh for that same date.
  this allows the pipeline to trigger multiple times a day as different partners
  (magnite, triplelift, etc.) deliver their files at different times, without
  re-processing dates that are already fully allocated.

  output: exactly one row with:
    - has_new_data (boolean) — true if at least one unprocessed (partner, date) pair exists.
    - target_date  (date)    — the earliest date that requires processing; null if none.
*/

with arriving_partners as (
  -- 1. collect all (partner, date) combinations that have landed in staging
  --    starting from the current increment (partition pruning to limit scan cost).
  select distinct
    report_date,
    partner_name
  from `minute-media-490214.minute_media_DWH.fact_demand_partner_reports`
  where report_date >= date('{{ ti.xcom_pull(task_ids="demand_partner_get_increment")[0][0] }}')
),

processed_partners as (
  -- 2. collect (partner, date) combinations already allocated in the dwh.
  --    partition pruning mirrors the staging filter to keep bytes processed low.
  --    filter on network (not demand_owner) because events_stream now maps payingEntity
  --    to the network field for prebid events, so 'Triplelift', 'IndexExchange', etc.
  --    appear in network — not in demand_owner (which remains the org owner, e.g. 'SI').
  select distinct
    event_date,
    network
  from `minute-media-490214.minute_media_DWH.fact_consolidated_revenue_hourly`
  where event_date  >= '{{ ti.xcom_pull(task_ids="demand_partner_get_increment")[0][0] }}'
    and actual_revenue is not null
    and network in (select partner_name from arriving_partners)
),

unprocessed_dates as (
  -- 3. anti-join: retain only staging rows with no matching dwh entry.
  --    each surviving row represents a partner that still needs allocation for that date.
  select
    a.report_date
  from arriving_partners  a
  left join processed_partners p
    on  a.partner_name = p.network
    and a.report_date  = p.event_date
  where p.event_date is null
)

-- 4. collapse to a single deterministic row for the continue_if_new_data operator.
--    min(report_date) picks the oldest unprocessed date so the pipeline
--    always works forward in chronological order.
--    the boolean flag is derived from whether any unprocessed date was found.
select
  (min(report_date) is not null) as has_new_data,
  min(report_date) as increment_column
from unprocessed_dates