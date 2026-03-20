create or replace table `minute-media-490214`.`minute_media_STG`.`GAM_grouping`
partition by event_date
cluster by event_hour, ad_unit_id, line_item, country
as

-- Country standardization: Joined with dim_country_mapping to resolve the format mismatch between GAM (full name) and the DWH (ISO code).
select
    date(g.timestamp) as event_date,
    timestamp_trunc(g.timestamp, hour) as event_hour,
    g.ad_unit_id,
    g.line_item_id as line_item,
    coalesce(m.country_code, g.country_name) as country,
    sum(g.impressions) as total_gam_impressions,
    sum(g.cpm_usd / 1000) as total_gam_revenue,
    max(g.timestamp) as max_gam_timestamp

from `minute-media-490214`.`minute_media_STG`.`GAM_pull_data` g

left join `minute-media-490214.minute_media_DWH.dim_country_mapping` m 
  on g.country_name = m.country_name
group by all