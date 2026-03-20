select coalesce(timestamp_trunc(max(max_events_stream_timestamp), hour), cast('1970-01-01 00:00:00 UTC' as timestamp)) as increment_column

from `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`

