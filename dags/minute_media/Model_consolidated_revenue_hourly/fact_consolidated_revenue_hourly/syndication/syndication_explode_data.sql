create or replace table `minute-media-490214`.`minute_media_STG`.`syndication_explode_data`
partition by event_date
cluster by event_hour, event, organization_id, network
as

select
  d.event_date,
  event_hour,
  d.event,
  d.organization_id,
  cast(null as string) as adunit,
  cast(null as string) as media_type,
  d.network,
  cast(null as string) as domain,
  cast(null as string) as line_item,
  cast(null as string) as advertiser,
  cast(null as string) as ad_deal_type,
  d.demand_owner,
  cast(null as string) as country,
  cast(null as int64) as tracked_event_count,
  -- evenly spread daily article count across 24 hours using integer floor division.
  -- the remainder (mod) is assigned to the first hour to guarantee exact conservation:
  -- sum(reconciled_event_count) across all 24 hours = original daily article_count.
  div(article_count, 24)
    + case when row_number() over (
        partition by d.event_date, d.organization_id, d.demand_owner
        order by event_hour
      ) = 1 then mod(article_count, 24) else 0 end
    as reconciled_event_count,
  d.total_revenue / 24 as actual_revenue,
  cast(null as float64) as estimated_revenue,
  max_syndication_date
  
from `minute-media-490214`.`minute_media_STG`.`syndication_pull_data` d
cross join unnest(
  generate_timestamp_array(
    timestamp(d.event_date),
    timestamp_add(timestamp(d.event_date), interval 23 hour),
    interval 1 hour
  )
) as event_hour
