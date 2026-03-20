create or replace table `minute-media-490214`.`minute_media_STG`.`events_stream_pull_data`
partition by event_date
cluster by line_item
as

select
    date(timestamp) as event_date,
    timestamp_trunc(timestamp, hour) as event_hour,
    timestamp, --- Raw timestamp for granular investigation
    event,
    organizationId as organization_id,
    adunit,
    mediaType as media_type,
    ---- Prebid is the auction platform. The actual paying partner (Magnite, IndexExchange..) is stored in payingEntity.
    case when network = 'Prebid' then payingEntity
         else network
    end as network,
    regexp_replace(net.host(url), r'^www\.', '') as domain,
    lineItem as line_item,
    adDealType as ad_deal_type,
    demandOwner as demand_owner,
    country,
    (cpm/1000) as revenue, -- CPM is cost per 1,000 impressions, so we divide by 1000 to get revenue per single event.
    case when network='MinuteSSP' then 1 else 0 end as is_actual_revenue --- All CPM values are estimated, except for 'MinuteSSP' network which is actual 

from `minute-media-490214`.`minute_media_DWH`.`events_stream`

where timestamp > cast('{{ ti.xcom_pull(task_ids="events_stream_get_increment")[0][0] }}' as timestamp)
  and timestamp <= timestamp_add(cast('{{ ti.xcom_pull(task_ids="events_stream_get_increment")[0][0] }}' as timestamp), interval 5 day)

