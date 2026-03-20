merge into `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly` as target
using `minute-media-490214`.`minute_media_STG`.`events_stream_grouping` as source
on target.event_date = source.event_date
  and target.event_hour = source.event_hour
  and target.event = source.event
  and target.organization_id = source.organization_id
  and coalesce(target.adunit, 'NA') = coalesce(source.adunit, 'NA')
  and coalesce(target.media_type, 'NA') = coalesce(source.media_type, 'NA')
  and coalesce(target.network, 'NA') = coalesce(source.network, 'NA')
  and coalesce(target.domain, 'NA') = coalesce(source.domain, 'NA')
  and coalesce(target.line_item, 'NA') = coalesce(source.line_item, 'NA')
  and coalesce(target.advertiser, 'NA') = coalesce(source.advertiser, 'NA')
  and coalesce(target.ad_deal_type, 'NA') = coalesce(source.ad_deal_type, 'NA')
  and coalesce(target.demand_owner, 'NA') = coalesce(source.demand_owner, 'NA')
  and coalesce(target.country, 'NA') = coalesce(source.country, 'NA')
  -- Partition pruning: Limits target table scan to recent days to optimize compute and handle late-arriving data.
  and target.event_date >= date_sub(source.event_date, interval 1 day) 


when matched then update 
set
    target.tracked_event_count = source.tracked_event_count,
    target.estimated_revenue = source.estimated_revenue,
    -- protect actual_revenue updated by downstream partner pipelines (like GAM). 
    -- events stream only overwrites this column if it is the owner (MinuteSSP).
    target.actual_revenue = case 
        when source.network = 'MinuteSSP' then source.actual_revenue 
        else target.actual_revenue 
    end,
    target.max_events_stream_timestamp = source.max_events_stream_timestamp,
    target.dwh_updated_at = current_timestamp()

when not matched then
  insert (
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
    tracked_event_count,
    actual_revenue,
    estimated_revenue,
    max_events_stream_timestamp,
    dwh_updated_at,
    dwh_created_at
  )
  values (
    source.event_date,
    source.event_hour,
    source.event,
    source.organization_id,
    source.adunit,
    source.media_type,
    source.network,
    source.domain,
    source.line_item,
    source.advertiser,
    source.ad_deal_type,
    source.demand_owner,
    source.country,
    source.tracked_event_count,
    source.actual_revenue,
    source.estimated_revenue,
    source.max_events_stream_timestamp,
    current_timestamp(),
    current_timestamp()
  )