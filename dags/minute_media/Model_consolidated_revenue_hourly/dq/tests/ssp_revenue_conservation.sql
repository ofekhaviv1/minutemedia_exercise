-- DQ check: every SSP STG row should have a matching SSOT row with the same actual_revenue.
-- Uses a grain-level JOIN (not date-level) to avoid counting old SSP rows from prior runs.
with stg_total as (
  select round(sum(actual_revenue), 4) as total
  from `minute-media-490214.minute_media_STG.ssp_explode_data`
),

matched_total as (
  select round(sum(t.actual_revenue), 4) as total
  from `minute-media-490214.minute_media_STG.ssp_explode_data` s
  join `minute-media-490214.minute_media_DWH.fact_consolidated_revenue_hourly` t
    on t.event_date = s.event_date
    and t.event_hour = s.event_hour
    and t.event = s.event
    and t.organization_id = s.organization_id
    and coalesce(t.adunit, 'NA') = coalesce(s.adunit, 'NA')
    and coalesce(t.media_type, 'NA') = coalesce(s.media_type, 'NA')
    and coalesce(t.network, 'NA') = coalesce(s.network, 'NA')
    and coalesce(t.domain, 'NA') = coalesce(s.domain, 'NA')
    and coalesce(t.line_item, 'NA') = coalesce(s.line_item, 'NA')
    and coalesce(t.advertiser, 'NA') = coalesce(s.advertiser, 'NA')
    and coalesce(t.ad_deal_type, 'NA') = coalesce(s.ad_deal_type, 'NA')
    and coalesce(t.demand_owner, 'NA') = coalesce(s.demand_owner, 'NA')
    and coalesce(t.country, 'NA') = coalesce(s.country, 'NA')
)

select
  cast(stg_total.total as string) as expected_value,
  cast(matched_total.total as string) as actual_value,
  abs(coalesce(stg_total.total, 0) - coalesce(matched_total.total, 0)) = 0 as passed
from stg_total cross join matched_total
