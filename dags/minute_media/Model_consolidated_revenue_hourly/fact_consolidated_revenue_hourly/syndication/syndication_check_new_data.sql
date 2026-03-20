-- Check if new 3rd-party syndication revenue data has arrived (usually at 09:00 AM).
-- If true, the ShortCircuitOperator will proceed; if false, it will skip downstream tasks.
select 
  case when count(*) > 0 then true else false end as has_new_data
from `minute-media-490214.minute_media_DWH.fact_syndication_revenue`
where transaction_date > date('{{ ti.xcom_pull(task_ids="syndication_get_increment")[0][0] }}')
