-- DQ check: no duplicate rows at the full 13-dimension grain.
-- A duplicate means a MERGE ON clause matched incorrectly.
select
  0 as expected_value,
  count(*) as actual_value,
  count(*) = 0 as passed
from (
  select
    event_date, event_hour, event, organization_id,
    coalesce(adunit, 'NA'), coalesce(media_type, 'NA'), coalesce(network, 'NA'),
    coalesce(domain, 'NA'), coalesce(line_item, 'NA'), coalesce(advertiser, 'NA'),
    coalesce(ad_deal_type, 'NA'), coalesce(demand_owner, 'NA'), coalesce(country, 'NA')
  from `minute-media-490214.minute_media_DWH.fact_consolidated_revenue_hourly`
  group by all
  having count(*) > 1
)
