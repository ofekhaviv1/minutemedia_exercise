create or replace table `minute-media-490214.minute_media_STG.demand_partner_allocation`
partition by event_date
cluster by event_hour, event, organization_id, country
as

with allocation_base as (

select
  dwh.*,
  g.total_ssp_impressions,
  g.total_ssp_revenue_usd,
  g.max_demand_partner_date,
  
  -- dynamic denominator: handles mixed granularity for the proportional weight calculation.
  -- if the partner didn't send an ad unit (e.g., magnite), map.adunit_path is null, 
  -- and it defaults to 'all_units'. this forces the window function to sum the traffic 
  -- of the entire site together, creating a single, site-wide denominator.
  -- if they did send an ad unit (e.g., triplelift), it only sums the traffic for that specific mapped banner.
  sum(dwh.tracked_event_count) over (
    partition by 
      g.event_date, 
      g.network, 
      g.organization_id, 
      g.country, 
      coalesce(map.adunit_path, 'all_units') 
  ) as total_tracked_for_group

from `minute-media-490214.minute_media_STG.demand_partner_grouping` g

-- 1. translation layer: map ssp aliases 'homepage_top' to real dwh paths '/home/top'
left join `minute-media-490214.minute_media_DWH.dim_ad_unit_mapping` map
  on lower(g.network) = lower(map.partner_name)
  and lower(g.organization_id) = lower(map.property_code)
  and lower(g.adunit) = lower(map.partner_ad_unit_name)

-- 2. logic layer: connect the revenue with the actual dwh traffic
join `minute-media-490214.minute_media_STG.demand_partner_last_data_dwh` dwh
  on dwh.event_date = g.event_date
  and dwh.network = g.network
  and dwh.organization_id = g.organization_id
  and dwh.country = g.country
  
  and (
    -- case a: partner sent no ad unit (null), OR partner sent an ad unit that is missing
    -- from dim_ad_unit_mapping (map.adunit_path is null in both situations via the left join).
    -- in either case, fall back to site-wide broadcast: replicate this revenue row to *all*
    -- ad units in the site. the 'all_units' window partition above computes the correct denominator.
    map.adunit_path is null

    -- case b: partner sent an ad unit AND it's in the mapping table. match exactly.
    or (map.adunit_path is not null and dwh.adunit = map.adunit_path)
  )
)

select
  event_date,
  event_hour,
  event,
  organization_id,
  adunit,
  media_type,
  network,
  domain,
  line_item,
  advertiser,
  ad_deal_type,
  demand_owner,
  country,
  max_demand_partner_date,
  tracked_event_count,
  -- relative weight: how much traffic did this specific hourly granularity generate
  /* =========================================================
     dbt insight: this null-safe proportional weight pattern is
     repeated across GAM and demand_partner pipelines.
     in dbt, extract as a macro:
       {{ proportional_weight('tracked_event_count', 'total_tracked_for_group') }}
     ========================================================= */
  coalesce(tracked_event_count / nullif(total_tracked_for_group, 0), 0) as weight,

  -- allocate ssp totals to hourly grain based on the calculated weight.
  cast(round(total_ssp_impressions * coalesce(tracked_event_count / nullif(total_tracked_for_group, 0), 0)) as int64) as allocated_reconciled_event_count,
  total_ssp_revenue_usd * coalesce(tracked_event_count / nullif(total_tracked_for_group, 0), 0) as allocated_actual_revenue
  
from allocation_base