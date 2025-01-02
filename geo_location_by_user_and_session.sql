select
    user_pseudo_id, 
    concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) as session_id,
    max(coalesce(
    (select value.string_value from unnest(event_params) where key = 'session_engaged'),
    cast((select value.int_value from unnest(event_params) where key = 'session_engaged') as string)
    )) as session_engaged, 
    -- geo.continent (dimension | the continent from which events were reported, based on ip address)
    array_agg(geo.continent)[safe_offset(0)] as continent, 
    -- geo.sub_continent (dimension | the subcontinent from which events were reported, based on ip address)
    array_agg(geo.sub_continent)[safe_offset(0)] as sub_continent,
    -- geo.country (dimension | the country from which events were reported, based on ip address)
    array_agg(geo.country)[safe_offset(0)] as country,
     -- geo.region (dimension | the region from which events were reported, based on ip address)
    array_agg(geo.region)[safe_offset(0)] as region,
    -- geo.city (dimension | the city from which events were reported, based on ip address)
    array_agg(geo.city)[safe_offset(0)] as city,
    -- geo.metro (dimension | the metro from which events were reported, based on ip address)
    array_agg(geo.metro)[safe_offset(0)] as metro
from
    -- change this to your google analytics 4 export location in bigquery
    `loc-bigquery.analytics_314964580.events_*`
where
  regexp_extract(_table_suffix, '[0-9]+') between format_date('%Y%m%d', date_sub(current_date(), interval 30 day)) and format_date('%Y%m%d', current_date())
group by
    user_pseudo_id, 
    session_id