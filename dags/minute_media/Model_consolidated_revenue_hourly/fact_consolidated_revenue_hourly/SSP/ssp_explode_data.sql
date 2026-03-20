create or replace table `minute-media-490214.minute_media_STG.ssp_explode_data`
partition by event_date
cluster by event_hour, event, organization_id, network
as

with daily_mapped as (
  /*
    step 1: standardize daily data.
    maps 3-letter country codes to the 2-letter iso standard used in the dwh.
  */
  select
    pop.report_date as event_date,
    pop.publisher_id as organization_id,
    pop.placement_type as media_type,
    'MinuteSSP' as network,
    'served' as event,
    
    -- standardizes country code: keeps 2-letter codes as-is, translates 3-letter codes using dim table
    coalesce( 
      case when length(pop.country_code) = 2 then upper(pop.country_code) end,
      map.country_code
    ) as country,
    
    pop.impressions,
    pop.revenue_usd,
    pop.max_ssp_date
  from `minute-media-490214.minute_media_STG.ssp_pull_data` pop
  
  -- strict join on alpha3 to avoid failing if the ssp sends a 2-letter code
  left join `minute-media-490214.minute_media_DWH.dim_country_mapping` map
    on upper(pop.country_code) = map.country_code_alpha3
)

/*
  step 2: hourly smearing.
  explodes the daily rows into 24 hourly rows and divides metrics evenly.
*/
select
  event_date,
  event_hour, -- generated timestamp from the unnest array
  event,
  organization_id,
  
  -- null placeholders to match the final dwh fact table schema
  cast(null as string) as adunit,
  media_type,
  network,
  cast(null as string) as domain,
  cast(null as string) as line_item,
  cast(null as string) as advertiser,
  cast(null as string) as ad_deal_type,
  cast(null as string) as demand_owner,
  country,
  cast(null as int64) as tracked_event_count,
  -- evenly spread daily impressions across 24 hours using integer floor division.
  -- the remainder (mod) is assigned to the first hour to guarantee exact conservation:
  -- sum(reconciled_event_count) across all 24 hours = original daily impressions.
  div(impressions, 24)
    + case when row_number() over (
        partition by event_date, organization_id, media_type, country
        order by event_hour
      ) = 1 then mod(impressions, 24) else 0 end
    as reconciled_event_count,
  revenue_usd / 24 as actual_revenue,
  cast(null as float64) as estimated_revenue,
  max_ssp_date

from daily_mapped

-- creates an array of 24 hourly timestamps for the current date and explodes it into rows
cross join unnest(
  generate_timestamp_array(
    timestamp(event_date),
    timestamp_add(timestamp(event_date), interval 23 hour),
    interval 1 hour
  )
) as event_hour