create or replace table `minute-media-490214`.`minute_media_STG`.`GAM_last_data_dwh`
partition by event_date
cluster by event_hour, adunit, line_item, country
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

from `minute-media-490214.minute_media_DWH.fact_consolidated_revenue_hourly`
where network = 'GAM' 
    and event = 'served'
    --We use the exact same Sequential Processing logic (Increment + 1) 
    -- applied in the GAM pull. This ensures 1:1 temporal alignment between the revenue 
    and event_date = date_add(date('{{ ti.xcom_pull(task_ids="gam_get_increment")[0][0] }}'), interval 1 day)