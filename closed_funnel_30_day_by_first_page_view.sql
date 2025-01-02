WITH prep as (
select
  user_pseudo_id,
  (select event_params.value.int_value from unnest(event_params) as event_params where event_params.key = 'ga_session_id') as session_id, 
  event_name,
  event_timestamp,
  (select value.string_value from unnest(event_params) where key = 'page_title') as page_title
from
  `loc-bigquery.analytics_314964580.events_*`  
where
  regexp_extract(_table_suffix, '[0-9]+') between format_date('%Y%m%d', date_sub(current_date(), interval 30 day)) and format_date('%Y%m%d', current_date())
  and
  event_name in ('page_view', 'add_to_cart', 'purchase')
order by
  user_pseudo_id,
  session_id,
  event_timestamp
),

prep_2 as (
select
  *,
  first_value(case when event_name = 'page_view' then page_title else null end) over(partition by user_pseudo_id, session_id order by event_timestamp) as first_page_view
from
  prep),

prep_3 as (
select
  user_pseudo_id,
  session_id,
  max(first_page_view) AS first_page_view,
  min(case when event_name = 'page_view' then event_timestamp end) as first_page_view_timestamp,
  min(case when event_name = 'add_to_cart' then event_timestamp end) as first_add_to_cart_timestamp,
  min(case when event_name = 'purchase' then event_timestamp end) AS first_purchase_timestamp
from
  prep_2
group by
  user_pseudo_id,
  session_id
having
  first_page_view_timestamp is not null
  and (first_add_to_cart_timestamp is null or first_add_to_cart_timestamp > first_page_view_timestamp)
  and (first_purchase_timestamp is null or first_purchase_timestamp > first_add_to_cart_timestamp)
)

select
  'all_pages' as all_pages,
  regexp_extract(first_page_view,r'^(.*?)\s*\|') as first_page_view,
  count(concat(user_pseudo_id, session_id)) as sessions_page_view,
  count(case when first_add_to_cart_timestamp is not null then concat(user_pseudo_id, session_id) else null end) as sessions_add_to_cart,
  count(case when first_purchase_timestamp is not null then session_id else null end) as sessions_purchase
from
  prep_3
group by
  first_page_view
 