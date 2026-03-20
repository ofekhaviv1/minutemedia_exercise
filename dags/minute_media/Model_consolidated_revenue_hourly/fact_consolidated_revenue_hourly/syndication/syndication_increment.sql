select coalesce(max(max_syndication_date), date('1970-01-01')) as increment_column

from `minute-media-490214`.`minute_media_DWH`.`fact_consolidated_revenue_hourly`

where network = 'Syndication'
