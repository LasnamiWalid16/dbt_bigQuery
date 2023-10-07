WITH _source as (
SELECT 
event_date, event_timestamp, event_name,user_pseudo_id,
MAX(CASE WHEN params.key = 'ga_session_number' THEN params.value.int_value	ELSE NULL END ) OVER (PARTITION BY event_timestamp,user_pseudo_id) as ga_session_number,
MAX(CASE WHEN params.key = 'page_title'   THEN params.value.string_value ELSE NULL END ) OVER (PARTITION BY event_timestamp,user_pseudo_id) as page_title,
MAX(CASE WHEN params.key = 'page_location' THEN params.value.string_value ELSE NULL END ) OVER (PARTITION BY event_timestamp,user_pseudo_id) as page_location,
MAX(CASE WHEN params.key = 'page_referrer' THEN params.value.string_value ELSE NULL END ) OVER (PARTITION BY event_timestamp,user_pseudo_id) as page_referrer,
MAX(CASE WHEN params.key = 'link_url' THEN params.value.string_value ELSE NULL END ) OVER (PARTITION BY event_timestamp,user_pseudo_id) as link_url,
MAX(CASE WHEN params.key = 'link_domain' THEN params.value.string_value ELSE NULL END ) OVER (PARTITION BY event_timestamp,user_pseudo_id) as link_domain,
MAX(CASE WHEN params.key = 'engagement_time_msec'   THEN params.value.int_value ELSE NULL END ) OVER (PARTITION BY event_timestamp,user_pseudo_id) as engagement_msec,
geo.country as country,


 FROM `ga4bigquery-364118.analytics_319124710.events_*`,
 UNNEST (event_params) as params
 WHERE  event_name = 'click'

), event_aggregated as(
    SELECT
    event_date,
    event_ts,
    ga_session_number,
    user_pseudo_id,
    event_name,
    MAX(page_title) as page_title,
    MAX(page_location) as page_location,
    MAX(page_referrer) as page_referrer,
    MAX(link_url) as link_url,
    MAX(link_domain) as link_domain,
    MAX(engagement_msec),
    MAX(country) as country

    FROM _source
    GROUP BY 1,2,3,4,5
)

SELECT * FROM event_aggregated

WHERE ga_session_number <= 7
ORDER BY event_ts DESC