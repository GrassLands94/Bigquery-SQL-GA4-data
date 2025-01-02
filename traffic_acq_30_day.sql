with prep as (
select
  user_pseudo_id,
  concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) as session_id,
  min(parse_date('%Y%m%d', event_date)) as date,
  coalesce(array_agg((select value.string_value from unnest(event_params) where key = 'source') ignore nulls order by event_timestamp)[safe_offset(0)],'(direct)') as source,
  coalesce(array_agg((select value.string_value from unnest(event_params) where key = 'medium') ignore nulls order by event_timestamp)[safe_offset(0)],'(none)') as medium,
  coalesce(array_agg((select value.string_value from unnest(event_params) where key = 'campaign') ignore nulls order by event_timestamp)[safe_offset(0)],'(not set)') as campaign,
  max((select value.int_value from unnest(event_params) where event_name = 'session_start' and key = 'ga_session_number')) as session_number,
  max((select value.string_value from unnest(event_params) where key = 'session_engaged')) as session_engaged,
  max((select value.int_value from unnest(event_params) where key = 'engagement_time_msec')) as engagement_time_msec,
  count(event_name) as event_count,
  -- change event_name to the event(s) you want to count
  --countif(event_name = 'click') as specific_event_count,
  -- change event_name to the conversion event(s) you want to count
  countif(event_name = 'purchase') as conversions,
  sum(ecommerce.purchase_revenue) as total_revenue
from
  `loc-bigquery.analytics_314964580.events_*`
where
  regexp_extract(_table_suffix, '[0-9]+') between format_date('%Y%m%d', date_sub(current_date(), interval 30 day)) and format_date('%Y%m%d', current_date())
group by
  user_pseudo_id,
  session_id
)

select
  date,
  case 
    when source = '(direct)' and (medium in ('(not set)','(none)')) then 'Direct'
    when regexp_contains(campaign, 'cross-network') then 'Cross-network'
    when (regexp_contains(source,'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart')
      or regexp_contains(campaign, '^(.*(([^a-df-z]|^)shop|shopping).*)$'))
      and regexp_contains(medium, '^(.*cp.*|ppc|paid.*)$') then 'Paid Shopping'
    when regexp_contains(source,'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex')
      and regexp_contains(medium,'^(.*cp.*|ppc|paid.*)$') then 'Paid Search'
    when regexp_contains(source,'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp')
      and regexp_contains(medium,'^(.*cp.*|ppc|paid.*)$') then 'Paid Social'
    when regexp_contains(source,'dailymotion|disneyplus|netflix|youtube|vimeo|twitch|vimeo|youtube')
      and regexp_contains(medium,'^(.*cp.*|ppc|paid.*)$') then 'Paid Video'
    when medium in ('display', 'banner', 'expandable', 'interstitial', 'cpm') then 'Display'
    when regexp_contains(source,'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart')
      or regexp_contains(campaign, '^(.*(([^a-df-z]|^)shop|shopping).*)$') then 'Organic Shopping'
    when regexp_contains(source,'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp')
      or medium in ('social','social-network','social-media','sm','social network','social media') then 'Organic Social'
    when regexp_contains(source,'dailymotion|disneyplus|netflix|youtube|vimeo|twitch|vimeo|youtube')
      or regexp_contains(medium,'^(.*video.*)$') then 'Organic Video'
    when regexp_contains(source,'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex')
      or medium = 'organic' then 'Organic Search'
    when regexp_contains(source,'email|e-mail|e_mail|e mail')
      or regexp_contains(medium,'email|e-mail|e_mail|e mail') then 'Email'
    when medium = 'affiliate' then 'Affiliates'
    when medium = 'referral' then 'Referral'
    when medium = 'audio' then 'Audio'
    when medium = 'sms' then 'SMS'
    when medium like '%push'
      or regexp_contains(medium,'mobile|notification') then 'Mobile Push Notifications'
    else 'Unassigned'
  end as session_default_channel_group,
  source as session_source,
  medium as session_medium,
  --concat(source,' / ',medium) as session_source_medium,
  campaign as session_campaign,
  count(distinct user_pseudo_id) as total_users,
  count(distinct case when engagement_time_msec > 0  or session_engaged = '1' then user_pseudo_id end) as active_users,
  count(distinct session_id) as sessions,
  count(distinct case when session_engaged = '1' then session_id end) as engaged_sessions,
  sum(engagement_time_msec/1000) as engagement_time,
  sum(event_count) as total_event_count,
  --sum(specific_event_count) as specific_event_count,
  sum(conversions) as conversions,
  ifnull(sum(total_revenue),0) as total_revenue
from
  prep
group by
  date,
  session_default_channel_group,
  session_source,
  session_medium,
  --,session_source_medium
  session_campaign
order by
  total_users desc