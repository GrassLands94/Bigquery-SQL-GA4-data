with user_id as (
select 
  user_pseudo_id, 
  max(user_id) as user_id
from
  `loc-bigquery.analytics_314964580.events_*`
where
  regexp_extract(_table_suffix, '[0-9]+') between format_date('%Y%m%d', date_sub(current_date(), interval 60 day)) and format_date('%Y%m%d', current_date())
group by
  user_pseudo_id
having 
  user_id is not null
)

select
  user_pseudo_id, 
  user_id, 
  c.renew, 
  c.new,
  c.lapsed
from
  user_id as u
inner join
  `loc-bigquery.analytics_314964580.buyer_behavior_by_const_id` as c
on
  cast(user_id as int64) = const_id






                          