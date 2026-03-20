create or replace table `minute-media-490214`.`minute_media_STG`.`ssp_pull_data`
partition by report_date
cluster by country_code
as

select
  report_date,
  publisher_id,
  placement_type,
  site_type,
  upper(country_code) as country_code,
  impressions,
  revenue_usd,
  report_date as max_ssp_date

from `minute-media-490214`.`minute_media_DWH`.`fact_ssp_report`
-- inclusive increment (>=) is used because the increment value might have already advanced.
-- this ensures that if we rerun the process on the same day, we re-pull the current date.
where report_date >= '{{ ti.xcom_pull(task_ids="ssp_get_increment")[0][0] }}'
  and site_type in ('external', 'ext_player')
