merge into `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly` as target
using `minute-media-490214`.`minute_media_STG`.`ssp_explode_data` as source
on
  -- Partition pruning: limits target scan to relevant dates (handles late-arriving updates)
  target.event_date >= date_sub('{{ ti.xcom_pull(task_ids="ssp_get_increment")[0][0] }}' , interval 1 day)

  -- Strict grain match
  and target.event_date = source.event_date
  and target.event_hour = source.event_hour
  and target.event = source.event
  and target.organization_id = source.organization_id
  and coalesce(target.adunit, 'NA') = coalesce(source.adunit, 'NA')
  and coalesce(target.media_type, 'NA') = coalesce(source.media_type, 'NA')
  and target.network = source.network
  and coalesce(target.domain, 'NA') = coalesce(source.domain, 'NA')
  and coalesce(target.line_item, 'NA') = coalesce(source.line_item, 'NA')
  and coalesce(target.advertiser, 'NA') = coalesce(source.advertiser, 'NA')
  and coalesce(target.ad_deal_type, 'NA') = coalesce(source.ad_deal_type, 'NA')
  and coalesce(target.demand_owner, 'NA') = coalesce(source.demand_owner, 'NA')
  and coalesce(target.country, 'NA') = coalesce(source.country, 'NA')

when matched then
  update set
    target.reconciled_event_count = source.reconciled_event_count,
    target.actual_revenue = source.actual_revenue,
    target.max_ssp_date = source.max_ssp_date,
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
    reconciled_event_count,
    actual_revenue,
    estimated_revenue,
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
    source.reconciled_event_count,
    source.actual_revenue,
    source.estimated_revenue,
    current_timestamp(),
    current_timestamp()
  )
