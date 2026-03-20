-- Check if new external SSP daily report data has arrived.
-- If true, the ShortCircuitOperator will proceed; if false, it will skip downstream tasks.
select
  case when count(*) > 0 then true else false end as has_new_data
from `minute-media-490214.minute_media_DWH.fact_ssp_report`
where report_date > date('{{ ti.xcom_pull(task_ids="ssp_get_increment")[0][0] }}')
  and site_type in ('external', 'ext_player')
