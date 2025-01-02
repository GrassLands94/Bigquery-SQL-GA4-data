with prep as (
  select
    event_name, 
    event_timestamp,
    user_pseudo_id,
    user_id,
    event_date,
    (select event_params.value.int_value from unnest(event_params) as event_params where event_params.key = 'ga_session_id') as session_id
  from
    `loc-bigquery.analytics_314964580.events_*`
  where
    regexp_extract(_table_suffix, '[0-9]+') between format_date('%Y%m%d', date_sub(current_date(), interval 1 day)) and format_date('%Y%m%d', current_date())
), 

totaL_cart_price as (
  select
    user_pseudo_id, 
    (select event_params.value.int_value from unnest(event_params) as event_params where event_params.key = 'ga_session_id') as session_id, 
    sum(items.quantity * items.price) as total_cart_price
  from
    `loc-bigquery.analytics_314964580.events_*`
  cross join
    unnest(items) as items
  where
    regexp_extract(_table_suffix, '[0-9]+') between format_date('%Y%m%d', date_sub(current_date(), interval 1 day)) and format_date('%Y%m%d', current_date())
    and
    event_name = 'add_to_cart'
  group by
    user_pseudo_id, 
    session_id

),

item_ids_grouped as (
  select
    user_pseudo_id, 
    (select event_params.value.int_value from unnest(event_params) as event_params where event_params.key = 'ga_session_id') as session_id,
    items.item_id as item_id,
    items.item_name as item_name,
    sum(items.quantity) as item_quantity,
    max(items.price) as max_item_price,
    min(items.price) as min_item_price -- Add min price to compare
  from
    `loc-bigquery.analytics_314964580.events_*`
  cross join
    unnest(items) as items
  where
    regexp_extract(_table_suffix, '[0-9]+') between format_date('%Y%m%d', date_sub(current_date(), interval 1 day)) and format_date('%Y%m%d', current_date())
    and
    event_name = 'add_to_cart'
  group by
    user_pseudo_id, 
    session_id,
    items.item_name,
    items.item_id
),



abandoned_items as (
  select
    user_pseudo_id, 
    session_id,
    string_agg(
      case 
        when item_id <> 'Fees' and item_id <> 'Donation' then 
          case 
            -- Handle price range when min and max are different
            when min_item_price <> max_item_price then concat(item_quantity, " ticket(s) for ", item_name, ", for $", min_item_price, " - $", max_item_price, " each", '\n')
            else concat(item_quantity, " ticket(s) for ", item_name, ", for $", max_item_price, " each", '\n')
          end
        when item_id = 'Donation' then concat(item_quantity, " donation(s) for ", item_name, ", for $", max_item_price, " each", '\n') 
        else null end, "") as product_quantity_price, 
    
  from
    item_ids_grouped
  group by
    user_pseudo_id,
    session_id
)

select
  prep.user_pseudo_id,
  prep.session_id, 
  event_date, 
  max(user_id) as user_id,
  min(case when event_name = 'add_to_cart' then event_timestamp else null end) as add_to_cart_timestamp, 
  min(case when event_name = 'purchase' then event_timestamp else null end) as purchase_timestamp, 
  max(concat(
    product_quantity_price, "Your cart total: $", total_cart_price
  )) as product_quantity_price
  
from 
  prep
left join
  abandoned_items as ai
on 
  prep.user_pseudo_id = ai.user_pseudo_id
  and
  prep.session_id = ai.session_id
left join
  total_cart_price as tcp
on 
  prep.user_pseudo_id = tcp.user_pseudo_id
  and
  prep.session_id = tcp.session_id
group by
  prep.user_pseudo_id, 
  prep.session_id, 
  event_date
having 
  user_id is not null
  and 
  add_to_cart_timestamp is not null
  and
  purchase_timestamp is null;