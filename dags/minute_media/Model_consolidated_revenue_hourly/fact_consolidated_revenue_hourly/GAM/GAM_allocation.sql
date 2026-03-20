create or replace table `minute-media-490214`.`minute_media_STG`.`GAM_allocation`
partition by event_date
cluster by event_hour, event, organization_id, line_item
as

with joined_data as (
  -- 1. dim_ad_unit_mapping bridges DWH adunit paths to GAM numeric ad_unit_ids.
  -- 2. country is safely joined since it was standardized in the previous grouping step.
  select
    t.*,
    g.total_gam_impressions,
    g.total_gam_revenue,
    g.max_gam_timestamp,
    sum(t.tracked_event_count) over (
      partition by t.event_date, t.event_hour, t.line_item, t.country, coalesce(cast(map.ad_unit_id as string), 'all_units')
    ) as total_tracked_for_group

  from `minute-media-490214`.`minute_media_STG`.`GAM_last_data_dwh` t

  left join `minute-media-490214.minute_media_dwh.dim_ad_unit_mapping` map
    on lower(map.partner_name) = 'gam'
    and t.adunit = map.adunit_path

  join `minute-media-490214`.`minute_media_STG`.`GAM_grouping` g
    on t.event_date = g.event_date
    and t.event_hour = g.event_hour
    and coalesce(t.line_item, 'NA') = coalesce(g.line_item, 'NA')
    and coalesce(t.country, 'NA') = coalesce(g.country, 'NA')
    and map.ad_unit_id = cast(g.ad_unit_id as int64)
)

-- allocate gam's actual revenue and impressions proportionally based on the stream's tracked events
/* =========================================================
   dbt insight: this null-safe proportional weight pattern is
   repeated across GAM and demand_partner pipelines.
   in dbt, extract as a macro:
     {{ proportional_weight('tracked_event_count', 'total_tracked_for_group') }}
   ========================================================= */
select
  *,
  coalesce(tracked_event_count / nullif(total_tracked_for_group, 0), 0) as weight,
  cast(round(total_gam_impressions * coalesce(tracked_event_count / nullif(total_tracked_for_group, 0), 0)) as int64) as allocated_reconciled_event_count,
  total_gam_revenue * coalesce(tracked_event_count / nullif(total_tracked_for_group, 0), 0) as allocated_actual_revenue
  
from joined_data