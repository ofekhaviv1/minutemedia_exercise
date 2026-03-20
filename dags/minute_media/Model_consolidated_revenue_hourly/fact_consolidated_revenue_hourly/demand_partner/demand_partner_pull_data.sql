create or replace table `minute-media-490214`.`minute_media_STG`.`demand_partner_pull_data`
partition by report_date
as

select
  report_date,
  partner_name,
  property_code,
  ad_unit,
  geo,
  impressions,
  revenue_usd,
  report_date as max_demand_partner_date
  
from `minute-media-490214.minute_media_DWH.fact_demand_partner_reports`

where report_date = '{{ ti.xcom_pull(task_ids="demand_partner_check_new_data_get_increment")[0][0] }}'
