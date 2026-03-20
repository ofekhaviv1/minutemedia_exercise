create or replace table `minute-media-490214`.`minute_media_STG`.`events_stream_grouping`
partition by event_date
cluster by event_hour, event, organization_id, adunit
as

select
    pop.event_date,
    pop.event_hour,
    pop.event,
    pop.organization_id,
    pop.adunit,
    pop.media_type,
    pop.network,
    pop.domain,
    pop.line_item,
    pop.ad_deal_type,
    pop.demand_owner,
    pop.country,
    li.advertiser_name as advertiser,
    count(*) as tracked_event_count,
    -- Split revenue based on whether it is actual or estimated
    sum(case when is_actual_revenue=1 then pop.revenue end) as actual_revenue,
    sum(case when is_actual_revenue=0 then pop.revenue end) as estimated_revenue,
    -- Save the latest timestamp for the next incremental load
    max(pop.timestamp) as max_events_stream_timestamp

from `minute-media-490214.minute_media_STG.events_stream_pull_data` as pop

-- Map line_item to advertiser name
left join `minute-media-490214.minute_media_DWH.dim_line_items_mapping` as li
    on pop.line_item = li.line_item_id

group by all

