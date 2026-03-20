create or replace table `minute-media-490214`.`minute_media_STG`.`GAM_pull_data`
partition by date(timestamp)
cluster by country_name
as

select
    timestamp,
    session_id,
    ad_unit_id,
    line_item_id,
    country_name,
    impressions,
    cpm_usd
    
from `minute-media-490214.minute_media_DWH.fact_gam_data_transfer`
where 
    -- 1. Sequential Processing Logic: Pull the exact next day following the last successful run (Increment + 1).
    -- This ensures data continuity and prevents gaps in the DWH if a previous run failed or was skipped.
    date(timestamp) = date_add(date('{{ ti.xcom_pull(task_ids="gam_get_increment")[0][0] }}'), interval 1 day)

    -- 2. Data Completeness Guard: Prevent processing the current day's data, which is incomplete 
    -- due to the 8-hour GAM Data Transfer SLA. This maintains mathematical integrity for allocation 
    -- by ensuring we only calculate revenue weights once the full day's data has arrived.
    and date(timestamp) < current_date()