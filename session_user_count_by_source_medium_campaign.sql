with source_medium_campaign as (
select 
  user_pseudo_id, 
  (select event_params.value.int_value from unnest(event_params) as event_params where event_params.key = 'ga_session_id') as session_id,
  max(
    coalesce(
      (select value.string_value from unnest(event_params) where key = 'session_engaged'),
      cast((select value.int_value from unnest(event_params) where key = 'session_engaged') as string)
    ) -- here we use the coalesce function to: if the first argument is NULL, then it'll pull from the second argument.
  ) as session_engaged,
  max((select params.value.string_value from unnest(event_params) as params where key = 'source')) as source, 
  max((select params.value.string_value from unnest(event_params) as params where key = 'medium')) as medium, 
  max((select params.value.string_value from unnest(event_params) as params where key = 'campaign')) as campaign
from
  `loc-bigquery.analytics_314964580.events_*`
where
  regexp_extract(_table_suffix, '[0-9]+') between format_date('%Y%m%d', date_sub(current_date(), interval 60 day)) and format_date('%Y%m%d', current_date())
group  by
  user_pseudo_id,
  session_id
)

select 
  source,
  medium,
  campaign,
  count(distinct concat(user_pseudo_id, session_id)) as count_sessions, 
  count(distinct case when session_engaged = '1' then concat(user_pseudo_id, session_id) else null end) as count_engaged_sessions,
  count(distinct user_pseudo_id) as count_users
from
  source_medium_campaign
group by
  source, 
  medium, 
  campaign