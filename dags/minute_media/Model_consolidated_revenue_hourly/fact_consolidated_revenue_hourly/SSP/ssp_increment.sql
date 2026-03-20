select coalesce(max(max_ssp_date), date('1970-01-01')) as increment_column
  
from `minute-media-490214.minute_media_DWH.fact_consolidated_revenue_hourly`

-- isolate the incremental check strictly to external ssp traffic.
-- 'tracked_event_count is null' ensures we don't accidentally check o&o traffic 
-- that arrived via the event_stream.
where network = 'MinuteSSP'
  and tracked_event_count is null