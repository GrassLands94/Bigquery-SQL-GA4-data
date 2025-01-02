-- set variables for conversion period and aquisition period
with variables as (
select
  30 as conversion_period,
  30 as aquisition_period
), 

-- find sessions that converted, in this case event_names that = 'purchase'
-- then grab the timestamp of the conversion
-- then create a unique conversion_id by combining the timestamp of the purchase, and the user_pseudo_id
conversions_1 as (
select
  user_pseudo_id, 
  (select event_params.value.int_value from unnest(event_params) as event_params where event_params.key = 'ga_session_id') as session_id, 
  concat(user_pseudo_id, event_timestamp) as conversion_id,
  max(timestamp_micros(event_timestamp)) as conversion_timestamp, 
from
  `loc-bigquery.analytics_314964580.events_*`
cross join
  variables
where
  _table_suffix between format_date('%Y%m%d',date_sub(current_date(), interval conversion_period day))
  and format_date('%Y%m%d',date_sub(current_date(), interval 1 day))
  and
  event_name = 'purchase'
group by
  1,2,3),  


-- Assign row numbers to each session based on two conditions:
-- First, rank of the purchase event WITHIN the all the purchas events in a user session (yes, sometimes 1 session can have two or more conversions)
-- Second, rank of the purchasing session out of all sessions for the user_pseudo_id
conversions_2 as (
select
  *,
  row_number() over (partition by user_pseudo_id,session_id order by conversion_timestamp) as session_scoped_conversion_rank, 
  row_number() over (partition by user_pseudo_id order by conversion_timestamp) as user_scoped_conversion_rank
from
  conversions_1),

-- filter to find:
-- 1. the first purchase event of out of all purchase events for a session
-- 2. THEN, find the first session that had a purchase event (converted session) out of all session for a user_pseudo_id
conversions_3 as (
select 
  *
from 
  conversions_2
where
  session_scoped_conversion_rank = 1
  and
  user_scoped_conversion_rank = 1
),

-- Create table for sessions, extract the start time for the session and the session's source/medium/campaign
sessions as (
select
  user_pseudo_id, 
  (select event_params.value.int_value from unnest(event_params) as event_params where event_params.key = 'ga_session_id') as session_id, 
  min(timestamp_micros(event_timestamp)) as session_start_timestamp, 
  max(
    concat(
      coalesce((select event_params.value.string_value from unnest(event_params) as event_params where event_params.key = 'source'),'(direct)')
      ,'/'
      ,coalesce((select event_params.value.string_value from unnest(event_params) as event_params where event_params.key = 'medium'),'(none)')
      ,'/'
      ,coalesce((select event_params.value.string_value from unnest(event_params) as event_params where event_params.key = 'campaign'),'(none)')
    )) as source_medium_campaign
from  
  `loc-bigquery.analytics_314964580.events_*`
cross join
  variables
where
  _table_suffix between format_date('%Y%m%d',date_sub(current_date(), interval conversion_period + aquisition_period day))
  and format_date('%Y%m%d',date_sub(current_date(), interval 1 day))
  and 
  user_pseudo_id in (select user_pseudo_id from conversions_3) -- filter the Sessions table to just sessions that belong to user_pseudo_id that are found within the conversions table, as those are the only user_pseudo_id's we want to track over the time period. We're only interested in folks that converted! If we didn't do this, this Sessions table would return all session, regardless of if the session converted
group by
  user_pseudo_id, 
  session_id),

-- combine session table, conversions table, and variables table 
attribution_raw as (
select
  s.*, 
  conversion_id, 
  conversion_timestamp, 
  count(distinct s.session_id) over (partition by s.user_pseudo_id) as total_session_per_user, 
  row_number() over (partition by s.user_pseudo_id order by session_start_timestamp) session_number
from
  sessions as s
left join
  conversions_3 as c_3
on
  s.user_pseudo_id = c_3.user_pseudo_id
  and
  s.session_id = c_3.session_id
cross join
  variables
where
  s.session_start_timestamp <= (select max(conversion_timestamp) from conversions_3 as c_3 where s.user_pseudo_id = c_3.user_pseudo_id)
  and
  s.session_start_timestamp >= timestamp_add((select max(conversion_timestamp) from conversions_3 as c_3 where s.user_pseudo_id = c_3.user_pseudo_id), interval - aquisition_period day)
order by 
  s.user_pseudo_id,
  s.session_id, 
  s.session_start_timestamp
), 

first_click as (
select
  user_pseudo_id,
  conversion_id,
  source_medium_campaign,
  1 as attribution_weight
from
  attribution_raw
where
  session_number = 1
),

last_click as (
select
  user_pseudo_id, 
  conversion_id, 
  source_medium_campaign,
  1 as attribution_weight
from
  attribution_raw
where
  session_number = total_session_per_user
),

prep_last_non_direct_click as (
select
  user_pseudo_id,
  conversion_id,
  case
    when source_medium_campaign != '(direct)/(none)/(none)' then source_medium_campaign
    when source_medium_campaign = '(direct)/(none)/(none)' then last_value(nullif(source_medium_campaign,'(direct)/(none)/(none)') ignore nulls) over (partition by user_pseudo_id order by session_number)
  end as source_medium_campaign,
  1 as attribution_weight
from
  attribution_raw),

last_non_direct_click as (
select
  user_pseudo_id,
  conversion_id,
  coalesce(source_medium_campaign,'(direct)/(none)/(none)') as source_medium_campaign,
  attribution_weight
from prep_last_non_direct_click
where
  conversion_id is not null),

linear as (
select 
  user_pseudo_id, 
  max(conversion_id) over(partition by user_pseudo_id order by session_start_timestamp rows between unbounded preceding and unbounded following) as conversion_id,
  source_medium_campaign, 
  1/(max(session_number) over(partition by user_pseudo_id order by session_start_timestamp rows between unbounded preceding and unbounded following)) as attribution_weight

from
  attribution_raw as r1
order by 
  user_pseudo_id,
  session_start_timestamp),


time_decay as (
select 
  user_pseudo_id, 
  max(conversion_id) over(partition by user_pseudo_id order by session_start_timestamp rows between unbounded preceding and unbounded following) as conversion_id, 
  source_medium_campaign,
  case 
    when total_session_per_user = 1 then 1
    else safe_divide(power(2, session_number/total_session_per_user), sum(power(2, session_number/total_session_per_user)) over(partition by user_pseudo_id)) end as attribution_weight 
from
  attribution_raw),

position_based as (
select
  user_pseudo_id, 
  max(conversion_id) over(partition by user_pseudo_id order by session_start_timestamp rows between unbounded preceding and unbounded following) as conversion_id, 
  source_medium_campaign,
  case
    when total_session_per_user = 1 then 1
    when total_session_per_user = 2 then 0.5
    when total_session_per_user > 2 then (
      case
        when session_number = 1 then 0.4
        when session_number = total_session_per_user then 0.4
        else 0.2 / (total_session_per_user - 2) end) end as attribution_weight
from
  attribution_raw)

select
  'first click' as attribution_model,
  source_medium_campaign, 
  sum(attribution_weight) as attribution_weight
from
  first_click
group by
  attribution_model,
  source_medium_campaign
union all
select
  'last click' as attribution_model,
  source_medium_campaign, 
  sum(attribution_weight) as attribution_weight
from
  last_click
group by
  attribution_model,
  source_medium_campaign
union all
select
  'last non direct click' as attribution_model,
  source_medium_campaign,
  sum(attribution_weight) as attribution_weight
from
  last_non_direct_click
group by
  attribution_model,
  source_medium_campaign
union all
select
  'linear' as attribution_model,
  source_medium_campaign, 
  sum(attribution_weight) as attribution_weight
from
  linear
group by
  attribution_model,
  source_medium_campaign
union all
select
  'time decay' as attribution_model,
  source_medium_campaign, 
  sum(attribution_weight) as attribution_weight
from
  time_decay
group by
  attribution_model,
  source_medium_campaign
union all
select
  'position based' as attribution_model,
  source_medium_campaign, 
  sum(attribution_weight) as attribution_weight
from
  position_based
group by
  attribution_model,
  source_medium_campaign






