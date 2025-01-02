with prep as (
select
  user_pseudo_id,
  concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) as session_id,
  coalesce(traffic_source.source,'(direct)') as first_source,
  coalesce(traffic_source.medium,'(none)') as first_medium,
  coalesce(traffic_source.name,'(not set)') as first_campaign,
  min(parse_date('%Y%m%d', event_date)) as date,
  max((select value.int_value from unnest(event_params) where event_name = 'session_start' and key = 'ga_session_number')) as session_number,
  max(
    coalesce(
    (select value.string_value from unnest(event_params) where key = 'session_engaged')
    ,cast((select value.int_value from unnest(event_params) where key = 'session_engaged') as string)
    )) as session_engaged,
  max((select value.int_value from unnest(event_params) where key = 'engagement_time_msec')) as engagement_time_msec,
  count(event_name) as event_count,
  -- change event_name to the conversion event(s) you want to count
  countif(event_name = 'purchase') as conversions,
  sum(ecommerce.purchase_revenue) as total_revenue
from
  `loc-bigquery.analytics_314964580.events_*`
where
  regexp_extract(_table_suffix, '[0-9]+') between format_date('%Y%m%d', date_sub(current_date(), interval 30 day)) and format_date('%Y%m%d', current_date())
group by
    user_pseudo_id,
    session_id, 
    first_source,
    first_medium, 
    first_campaign
order by
  user_pseudo_id, 
  session_number
    )

select
  date,
  case 
    when first_source = '(direct)' and (first_medium in ('(not set)','(none)')) then 'Direct'
    when regexp_contains(first_campaign, 'cross-network') then 'Cross-network'
    when (regexp_contains(first_source,'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart')
      or regexp_contains(first_campaign, '^(.*(([^a-df-z]|^)shop|shopping).*)$'))
      and regexp_contains(first_medium, '^(.*cp.*|ppc|paid.*)$') then 'Paid Shopping'
    when regexp_contains(first_source,'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex')
      and regexp_contains(first_medium,'^(.*cp.*|ppc|paid.*)$') then 'Paid Search'
    when regexp_contains(first_source,'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp')
      and regexp_contains(first_medium,'^(.*cp.*|ppc|paid.*)$') then 'Paid Social'
    when regexp_contains(first_source,'dailymotion|disneyplus|netflix|youtube|vimeo|twitch|vimeo|youtube')
      and regexp_contains(first_medium,'^(.*cp.*|ppc|paid.*)$') then 'Paid Video'
    when first_medium in ('display', 'banner', 'expandable', 'interstitial', 'cpm') then 'Display'
    when regexp_contains(first_source,'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart')
      or regexp_contains(first_campaign, '^(.*(([^a-df-z]|^)shop|shopping).*)$') then 'Organic Shopping'
    when regexp_contains(first_source,'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp')
      or first_medium in ('social','social-network','social-media','sm','social network','social media') then 'Organic Social'
    when regexp_contains(first_source,'dailymotion|disneyplus|netflix|youtube|vimeo|twitch|vimeo|youtube')
      or regexp_contains(first_medium,'^(.*video.*)$') then 'Organic Video'
    when regexp_contains(first_source,'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex')
      or first_medium = 'organic' then 'Organic Search'
    when regexp_contains(first_source,'email|e-mail|e_mail|e mail')
      or regexp_contains(first_medium,'email|e-mail|e_mail|e mail') then 'Email'
    when first_medium = 'affiliate' then 'Affiliates'
    when first_medium = 'referral' then 'Referral'
    when first_medium = 'audio' then 'Audio'
    when first_medium = 'sms' then 'SMS'
    when first_medium like '%push'
      or regexp_contains(first_medium,'mobile|notification') then 'Mobile Push Notifications'
    else 'Unassigned'
  end as first_user_default_channel_group,
  first_source as first_user_source,
  first_medium as first_user_medium,
  --concat(first_source,' / ',first_medium) as first_user_source_medium,
  first_campaign as first_user_campaign,
  count(distinct user_pseudo_id) as total_users,
  count(distinct case when engagement_time_msec > 0  or session_engaged = '1' then user_pseudo_id end) as active_users,
  count(distinct case when session_number = 1 then user_pseudo_id end) as new_users,  --distinct included, while logically not necessary, for data accuracy, as there may be some inconsistancies in the raw data 
  count(distinct case when session_number > 1 then user_pseudo_id end) as returning_users,
  count( session_id) as sessions, -- removed distinct
  count( case when session_engaged = '1' then session_id end) as engaged_sessions,-- removed distinct
  sum(engagement_time_msec/1000) as engagement_time,
  sum(event_count) as total_event_count,
  --sum(specific_event_count) as specific_event_count,
  sum(conversions) as conversions,
  ifnull(sum(total_revenue),0) as total_revenue
from
  prep
group by
  date,
  first_user_default_channel_group
  ,first_user_source
  ,first_user_medium
  --,first_user_source_medium
  ,first_user_campaign
order by
  total_users desc