-- DQ check: required dimension columns must never be NULL.
-- Only checks the 4 columns used WITHOUT COALESCE in every merge ON clause.
-- network, adunit, domain etc. are nullable dimensions (merged via COALESCE).
select
  0 as expected_value,
  count(*) as actual_value,
  count(*) = 0 as passed
from `minute-media-490214.minute_media_DWH.fact_consolidated_revenue_hourly`
where event_date is null
  or event_hour is null
  or event is null
  or organization_id is null
