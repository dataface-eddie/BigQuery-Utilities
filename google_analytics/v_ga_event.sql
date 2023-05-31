CREATE OR REPLACE VIEW analytics.v_ga_event AS
SELECT event_date
  , REPLACE(
      SPLIT(
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location')
        , '.com')[SAFE_OFFSET(0)]
      , 'https://www.', '') AS hostname
  , traffic_source.name AS acquisition_campaign
  , traffic_source.source acquisition_source
  , traffic_source.medium acquisiiton_medium
  , (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') AS campaign
  , (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') AS source
  , (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') AS medium
  , (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location
  , collected_traffic_source.manual_campaign_name
  , event_name
  , COUNT(*) AS events
  , user_pseudo_id
  , COUNTIF(event_name = "first_visit") AS new_users
  , COUNTIF(event_name = "session_start") AS sessions
  , SUM((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engaged_session_event')) AS engaged_session_event
  , SUM((SELECT value.int_value FROM UNNEST(event_params) WHERE key = '	engagement_time_msec')) AS engagement_time_msec
FROM `<project>.analytics.events_*`
GROUP BY event_date
  , hostname
  , traffic_source.name
  , traffic_source.source
  , traffic_source.medium
  , collected_traffic_source.manual_campaign_name
  , campaign
  , source
  , medium
  , page_location
  , event_name
  , user_pseudo_id
