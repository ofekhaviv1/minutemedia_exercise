-- DQ check: demand partner allocated revenue in SSOT should not exceed source revenue.
-- Small overallocation (< 0.1%) is tolerated due to float precision in proportional allocation.
with stg as (
  select round(sum(total_ssp_revenue_usd), 4) as source_total
  from `minute-media-490214.minute_media_STG.demand_partner_grouping`
),

ssot as (
  select round(sum(actual_revenue), 4) as allocated_total
  from `minute-media-490214.minute_media_DWH.fact_consolidated_revenue_hourly`
  where network not in ('GAM', 'MinuteSSP')
    and max_demand_partner_date is not null
    and event_date in (
      select distinct event_date
      from `minute-media-490214.minute_media_STG.demand_partner_allocation`
    )
)

select
  cast(stg.source_total as string) as expected_value,
  cast(ssot.allocated_total as string) as actual_value,
  coalesce(ssot.allocated_total, 0) <= coalesce(stg.source_total, 0) * 1.001 as passed
from stg cross join ssot
