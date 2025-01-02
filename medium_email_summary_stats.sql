with prep as (
select 
  source,
  medium, 
  campaign,
  sum(donation_revenue) as total_donation_revenue, 
  sum(donation_quantity) as total_donation_quantity, 
  sum(product_revenue) as total_product_revenue, 
  sum(product_quantity) as total_product_quantity,
  sum(conversion_flag) as conversion_count
from
  `loc-bigquery.analytics_314964580.source_medium_campaign_by_session` as smc
left join
  `loc-bigquery.analytics_314964580.items_agg_by_session_last_30_days` as i
on
  smc.user_pseudo_id = i.user_pseudo_id
  and
  smc.session_id = i.session_id
where
  medium = 'email'
group by
  source,
  medium,
  campaign
)

select 
  prep.*, 
  count_sessions, 
  count_engaged_sessions,
  count_users as count_user_pseudo_id,
  bb.count_purchasing_user_pseudo_id, 
  bb.count_new_buyers, 
  bb.count_renew_buyers, 
  bb.count_lapsed_buyers, 
  c.ldate,
  cast(c.send_amt as int64) as send_amt,
  cast(c.opens as int64) as opens,                -- Total opens
  cast(c.uniqueopens as int64) as uniqueopens,    -- Unique opens
  cast(c.linkclicks as int64) as linkclicks,      -- Total link clicks
  cast(c.uniquelinkclicks as int64) as uniquelinkclicks,  -- Unique link clicks
  cast(c.subscriberclicks as int64) as subscriberclicks,  -- Subscriber-specific clicks
  cast(c.forwards as int64) as forwards,          -- Total forwards
  cast(c.uniqueforwards as int64) as uniqueforwards, -- Unique forwards
  cast(c.hardbounces as int64) as hardbounces,    -- Hard bounces
  cast(c.softbounces as int64) as softbounces,    -- Soft bounces
  cast(c.unsubscribes as int64) as unsubscribes,  -- Unsubscribes
  cast(c.unsubreasons as int64) as unsubreasons,  -- Reasons for unsubscribes (if applicable)
  cast(c.replies as int64) as replies,            -- Total replies
  cast(c.uniquereplies as int64) as uniquereplies, -- Unique replies
  cast(c.socialshares as int64) as socialshares   -- Social media shares

from 
  prep
left join
  `loc-bigquery.analytics_314964580.session_user_count_by_source_medium_campaign` as su
on
  prep.source = su.source
  and
  prep.medium = su.medium
  and
  prep.campaign = su.campaign
left join
  `analytics_314964580.buyer_behavior_by_source_medium_campaign` as bb
on
  prep.source = bb.source
  and
  prep.medium = bb.medium
  and
  prep.campaign = bb.campaign
left join
  `analytics_314964580.activecampaign_campaigns` as c
on
  prep.campaign = c.name
order by
  total_product_revenue desc






  