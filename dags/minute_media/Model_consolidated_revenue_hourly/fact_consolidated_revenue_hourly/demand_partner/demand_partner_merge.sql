{% set dp_increment = ti.xcom_pull(task_ids="demand_partner_check_new_data_get_increment") %}
{% if dp_increment is not none %}
merge into `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly` as target
using `minute-media-490214`.`minute_media_STG`.`demand_partner_allocation` as source
on
  -- Partition pruning: limit target scan to relevant dates (plus 1 day for late-arriving events)
  target.event_date = date('{{ ti.xcom_pull(task_ids="demand_partner_check_new_data_get_increment")[0][0] }}')

  and target.event_date = source.event_date
  and target.event_hour = source.event_hour
  and target.event = source.event
  and target.organization_id = source.organization_id
  and coalesce(target.adunit, 'NA') = coalesce(source.adunit, 'NA')
  and coalesce(target.media_type, 'NA') = coalesce(source.media_type, 'NA')
  and target.network = source.network
  and coalesce(target.domain, 'NA') = coalesce(source.domain, 'NA')
  and target.country = source.country

  -- Nullable buyer/deal attributes: must coalesce to avoid null-mismatch fan-out
  and coalesce(target.line_item, 'NA') = coalesce(source.line_item, 'NA')
  and coalesce(target.advertiser, 'NA') = coalesce(source.advertiser, 'NA')
  and coalesce(target.ad_deal_type, 'NA') = coalesce(source.ad_deal_type, 'NA')
  and coalesce(target.demand_owner, 'NA') = coalesce(source.demand_owner, 'NA')

when matched then
  update set
    target.reconciled_event_count = source.allocated_reconciled_event_count,
    target.actual_revenue = source.allocated_actual_revenue,
    target.max_demand_partner_date = source.max_demand_partner_date,
    target.dwh_updated_at = current_timestamp()
{% else %}
SELECT 1;  -- demand partner pipeline skipped, nothing to merge
{% endif %}
