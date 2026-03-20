create or replace table `minute-media-490214`.`minute_media_STG`.`syndication_pull_data`
partition by event_date
as

select
    transaction_date as event_date,
    content_property as organization_id,
    partner_name as demand_owner,
    'Syndication' as network,
    'syndication_view' as event,
    article_count,
    total_revenue,
    transaction_date as max_syndication_date


from `minute-media-490214`.`minute_media_DWH`.`fact_syndication_revenue`

where transaction_date >= '{{ ti.xcom_pull(task_ids="syndication_get_increment")[0][0] }}'
