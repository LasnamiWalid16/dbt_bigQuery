WITH _source as (
SELECT 
PARSE_DATE('%Y%m%d',event_date) as event_date, event_timestamp,TIMESTAMP_MICROS(event_timestamp) as event_ts, event_name,
user_pseudo_id,
MAX(CASE WHEN params.key = 'ga_session_number' THEN params.value.int_value	ELSE NULL END ) OVER (PARTITION BY event_timestamp,user_pseudo_id) as ga_session_number,
MAX(CASE WHEN params.key = 'page_title'   THEN params.value.string_value ELSE NULL END ) OVER (PARTITION BY event_timestamp,user_pseudo_id) as page_title,
MAX(CASE WHEN params.key = 'page_location' THEN params.value.string_value ELSE NULL END ) OVER (PARTITION BY event_timestamp,user_pseudo_id) as page_location,
MAX(CASE WHEN params.key = 'page_referrer' THEN params.value.string_value ELSE NULL END ) OVER (PARTITION BY event_timestamp,user_pseudo_id) as page_referrer,
MAX(CASE WHEN params.key = 'link_url' THEN params.value.string_value ELSE NULL END ) OVER (PARTITION BY event_timestamp,user_pseudo_id) as link_url,
MAX(CASE WHEN params.key = 'link_domain' THEN params.value.string_value ELSE NULL END ) OVER (PARTITION BY event_timestamp,user_pseudo_id) as link_domain,
MAX(CASE WHEN params.key = 'engagement_time_msec'   THEN params.value.int_value ELSE NULL END ) OVER (PARTITION BY event_timestamp,user_pseudo_id) as engagement_msec,

geo.continent as continent,
geo.country as country,
geo.region as region,
geo.city as city,

device.category as device_category,
device.mobile_brand_name as device_brand,
device.mobile_model_name as device_model,
device.mobile_marketing_name as mobile_marketing_name,
device.operating_system as device_os,
device.operating_system_version as device_os_version,
device.language as device_language,
device.is_limited_ad_tracking as device_is_limited_ad_tracking,
device.web_info.browser as mobile_language_web_info_browser,
device.web_info.browser_version as mobile_browser_version

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
    MAX(continent) as continent,
    MAX(country) as country,
    MAX(region) as region,
    MAX(city) as city,
    MAX(device_category) as device_category,
    MAX(device_brand) as device_brand,
    MAX(device_model) as device_model,
    MAX(mobile_marketing_name) as mobile_marketing_name,
    MAX(device_os) as device_os,
    MAX(device_os_version) as device_os_version,
    MAX(device_language) as device_language,
    MAX(device_is_limited_ad_tracking) as device_is_limited_ad_tracking,
    MAX(mobile_language_web_info_browser) as mobile_language_web_info_browser,
    MAX(mobile_browser_version) as mobile_browser_version

    FROM _source
    GROUP BY 1,2,3,4,5
)

SELECT * FROM event_aggregated

WHERE ga_session_number <= 7
ORDER BY event_ts DESC;