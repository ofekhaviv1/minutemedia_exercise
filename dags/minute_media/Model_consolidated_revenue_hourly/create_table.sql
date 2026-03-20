CREATE OR REPLACE TABLE `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly` (
  event_date DATE,
  event_hour TIMESTAMP,
  event STRING,
  organization_id STRING,
  adunit STRING,
  media_type STRING,
  network STRING,
  domain STRING,
  line_item STRING,
  advertiser STRING,
  ad_deal_type STRING,
  demand_owner STRING,
  country STRING,
  tracked_event_count INT64,
  reconciled_event_count INT64,
  actual_revenue FLOAT64,
  estimated_revenue FLOAT64,
  max_events_stream_timestamp TIMESTAMP,
  max_gam_timestamp TIMESTAMP,
  max_demand_partner_date DATE,
  max_ssp_date DATE,
  max_syndication_date DATE,
  dwh_updated_at TIMESTAMP,
  dwh_created_at TIMESTAMP
)
PARTITION BY 
  event_date
CLUSTER BY 
  organization_id, 
  network, 
  event, 
  country;