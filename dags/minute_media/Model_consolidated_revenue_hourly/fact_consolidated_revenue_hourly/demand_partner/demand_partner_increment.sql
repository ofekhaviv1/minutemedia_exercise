create or replace table `minute-media-490214`.`minute_media_STG`.`demand_partner_increment` as

select coalesce(max(max_demand_partner_date), cast('1970-01-01' as date)) as increment_column

from `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`