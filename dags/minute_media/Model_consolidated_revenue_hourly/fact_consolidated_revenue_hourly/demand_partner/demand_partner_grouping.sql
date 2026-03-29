create or replace table `minute-media-490214`.`minute_media_STG`.`demand_partner_grouping`
partition by event_date
cluster by organization_id, country, adunit
as

select
  report_date as event_date,                   
  partner_name as network,                     
  property_code as organization_id,            
  ad_unit as adunit,                           
  upper(geo) as country,                       
  sum(impressions) as total_demand_partner_impressions,
  sum(revenue_usd) as total_demand_partner_revenue_usd,
  max(max_demand_partner_date) as max_demand_partner_date

from `minute-media-490214`.`minute_media_STG`.`demand_partner_pull_data`

group by all
