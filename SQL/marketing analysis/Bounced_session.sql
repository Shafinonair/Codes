-- Get each event with the timestamp of the last event for each user
WITH EventsWithLastEvent AS (
  SELECT *,
    LAG(event_timestamp, 1) OVER (PARTITION BY user_pseudo_id, campaign ORDER BY event_timestamp) AS last_event
  FROM `tc-da-1.turing_data_analytics.raw_events`
),
-- Identify new sessions based on a gap of more than 1800 seconds (30 minutes)
Sessions AS (
  SELECT 
    user_pseudo_id,
    event_timestamp,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP_MICROS(event_timestamp)) AS weekday,
    campaign,
    SUM(
      CASE 
        WHEN TIMESTAMP_DIFF(TIMESTAMP_MICROS(event_timestamp), TIMESTAMP_MICROS(last_event), SECOND) > 1800 
        THEN 1
        ELSE 0
      END
    ) OVER (PARTITION BY user_pseudo_id, campaign ORDER BY event_timestamp) AS session_id
  FROM EventsWithLastEvent
),
-- Calculating the length of each session
SessionDurations AS (
  SELECT 
    user_pseudo_id,
    weekday,
    campaign,
    session_id,
    TIMESTAMP_DIFF(MAX(TIMESTAMP_MICROS(event_timestamp)), MIN(TIMESTAMP_MICROS(event_timestamp)), SECOND) AS session_length_seconds
  FROM Sessions
  GROUP BY user_pseudo_id, weekday, campaign, session_id
),
-- Calculating total sessions and bounced sessions
TotalAndBounced AS (
  SELECT 
    campaign,
    COUNT(*) AS total_sessions,
    SUM(CASE WHEN session_length_seconds < 3 THEN 1 ELSE 0 END) AS total_bounced_sessions
  FROM SessionDurations
  GROUP BY campaign
)
-- Calculating bounce rate
SELECT 
  campaign,
  total_sessions,
  total_bounced_sessions,
  (total_bounced_sessions * 100.0 / total_sessions) AS bounce_rate
FROM TotalAndBounced
WHERE campaign IN ('NewYear_V1', 'NewYear_V2', 'Holiday_V1', 'Holiday_V2', 'BlackFriday_V1', 'BlackFriday_V2'); --'(referral)','(organic)','(direct)'
