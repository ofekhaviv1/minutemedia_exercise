create or replace table `minute-media-490214`.`minute_media_STG`.`demand_partner_last_data_dwh`
partition by event_date
cluster by organization_id, country, adunit, event_hour
as

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
    tracked_event_count
    
  from `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`
  where event = 'served'
    and network not in ('GAM','MinuteSSP') 
    and event_date = date('{{ ti.xcom_pull(task_ids="demand_partner_check_new_data_get_increment")[0][0] }}')