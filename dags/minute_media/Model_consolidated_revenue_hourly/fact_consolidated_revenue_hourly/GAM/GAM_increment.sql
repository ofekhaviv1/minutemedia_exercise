select coalesce(max(max_gam_timestamp), cast('1970-01-01 00:00:00 UTC' as timestamp)) as increment_column

from `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`
