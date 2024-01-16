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
-- Calculate the length of each session
SessionDurations AS (
  SELECT 
    user_pseudo_id,
    weekday,
    campaign,
    session_id,
    TIMESTAMP_DIFF(MAX(TIMESTAMP_MICROS(event_timestamp)), MIN(TIMESTAMP_MICROS(event_timestamp)), SECOND) AS session_length_seconds
  FROM Sessions
  GROUP BY user_pseudo_id, weekday, campaign, session_id
)
-- Identify Bounced Sessions (less than 3 seconds)
SELECT 
  weekday,
  campaign,
  COUNT(*) AS total_bounced_sessions
FROM SessionDurations
WHERE session_length_seconds < 3
GROUP BY weekday, campaign;
