{% set gam_increment = ti.xcom_pull(task_ids="gam_get_increment") %}
{% if gam_increment is not none %}
merge into `minute-media-490214.minute_media_DWH.fact_consolidated_revenue_hourly` as target
using `minute-media-490214.minute_media_STG.GAM_allocation` as source
on 
  -- Partition pruning: limit target table scan to the relevant timeframe to optimize query costs.
  target.event_date >= date_sub(date('{{ ti.xcom_pull(task_ids="gam_get_increment")[0][0] }}'), interval 1 day)
  
  -- Exact match on all granularity dimensions to prevent data duplication (fan-out).
  and target.event_date = source.event_date
  and target.event_hour = source.event_hour
  and target.event = source.event
  and target.organization_id = source.organization_id
  
  -- Safety constraint: Ensure this pipeline exclusively updates GAM network records.
  and target.network = 'GAM'
  and target.network = source.network
  
  -- Handle NULL values safely for all dimensions that might be empty.
  -- This prevents BigQuery from failing to match rows when both source and target are NULL.
  and coalesce(target.adunit, 'NA') = coalesce(source.adunit, 'NA')
  and coalesce(target.media_type, 'NA') = coalesce(source.media_type, 'NA')
  and coalesce(target.domain, 'NA') = coalesce(source.domain, 'NA')
  and coalesce(target.country, 'NA') = coalesce(source.country, 'NA')

  -- In programmatic and open-market auctions, specific buyer details are often unknown,
  -- meaning fields like advertiser, line_item, or ad_deal_type can naturally be NULL.
  and coalesce(target.line_item, 'NA') = coalesce(source.line_item, 'NA')
  and coalesce(target.advertiser, 'NA') = coalesce(source.advertiser, 'NA')
  and coalesce(target.ad_deal_type, 'NA') = coalesce(source.ad_deal_type, 'NA')
  and coalesce(target.demand_owner, 'NA') = coalesce(source.demand_owner, 'NA')

-- Enrichment Only Strategy: GAM data arrives ~8 hours late and updates actual_revenue 
-- on top of existing events. We do NOT insert new rows (WHEN NOT MATCHED) to avoid 
-- creating "ghost rows" with revenue but zero tracked_event_count.
when matched then
  update set
    -- Idempotent overwrite: Safely replaces existing metrics if the pipeline is re-run.
    target.reconciled_event_count = source.allocated_reconciled_event_count,
    target.actual_revenue = source.allocated_actual_revenue,
    target.max_gam_timestamp = source.max_gam_timestamp, 
    target.dwh_updated_at = current_timestamp()
{% else %}
SELECT 1;  -- GAM pipeline skipped, nothing to merge
{% endif %}