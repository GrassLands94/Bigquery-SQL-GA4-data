select 
  user_pseudo_id, 
  (select event_params.value.int_value from unnest(event_params) as event_params where event_params.key = 'ga_session_id') as session_id,
  sum(case when items.item_id = 'Donation' or items.item_id = 'hd3g4y' then items.item_revenue else null end) as donation_revenue,
  sum(case when items.item_id = 'Donation' or items.item_id = 'hd3g4y' then items.quantity else null end) as donation_quantity,
  sum(case when items.item_id <> 'Donation' and items.item_id <> 'Fees' then items.item_revenue else null end) as product_revenue,
  sum(case when items.item_id <> 'Donation' and items.item_id <> 'Fees' then items.quantity else null end) as product_quantity, 
  max(case when event_name = 'purchase' then 1 else null end) as conversion_flag
from
  `loc-bigquery.analytics_314964580.events_*`
cross join
  unnest(items) as items
where
  regexp_extract(_table_suffix, '[0-9]+') between format_date('%Y%m%d', date_sub(current_date(), interval 60 day)) and format_date('%Y%m%d', current_date())
  and 
  event_name = 'purchase'
group by
  user_pseudo_id, 
  session_id