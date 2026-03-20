-- DQ check: revenue and count columns must never be negative.
select
  0 as expected_value,
  count(*) as actual_value,
  count(*) = 0 as passed
from `minute-media-490214.minute_media_DWH.fact_consolidated_revenue_hourly`
where actual_revenue < 0
  or estimated_revenue < 0
  or tracked_event_count < 0
  or reconciled_event_count < 0
