with source_medium_campaign as (
select 
  user_pseudo_id,
  (select event_params.value.int_value from unnest(event_params) as event_params where event_params.key = 'ga_session_id') as session_id,
  max((select params.value.string_value from unnest(event_params) as params where key = 'source')) as source, 
  max((select params.value.string_value from unnest(event_params) as params where key = 'medium')) as medium, 
  max((select params.value.string_value from unnest(event_params) as params where key = 'campaign')) as campaign, 
  max(case when event_name = 'purchase' then 1 else null end) as purchase_flag
from
  `loc-bigquery.analytics_314964580.events_*`
where
  regexp_extract(_table_suffix, '[0-9]+') between format_date('%Y%m%d', date_sub(current_date(), interval 60 day)) and format_date('%Y%m%d', current_date())
group  by
  user_pseudo_id,
  session_id
having 
  purchase_flag is not null
)


select 
  source,
  medium,
  campaign,
  count(distinct concat(smc.user_pseudo_id, session_id)) as count_purchasing_sessions, 
  count(distinct smc.user_pseudo_id) as count_purchasing_user_pseudo_id,
  count(distinct case when bbupi.new = 1 then smc.user_pseudo_id else null end) as count_new_buyers, 
  count(distinct case when bbupi.renew = 1 then smc.user_pseudo_id else null end) as count_renew_buyers,
  count(distinct case when bbupi.lapsed = 1 then smc.user_pseudo_id else null end) as count_lapsed_buyers
from
  source_medium_campaign as smc
left join
  `analytics_314964580.buyer_behavior_by_user_pseudo_id` as bbupi
on 
  smc.user_pseudo_id = bbupi.user_pseudo_id
group by
  source, 
  medium, 
  campaign
order by count_purchasing_user_pseudo_id desc