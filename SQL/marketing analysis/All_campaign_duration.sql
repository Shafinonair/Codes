WITH EventsWithLastEvent AS (
  SELECT *,
         LAG(event_timestamp, 1) OVER (PARTITION BY user_pseudo_id, campaign ORDER BY event_timestamp) AS last_event,
         EXTRACT(DAYOFWEEK FROM TIMESTAMP_MICROS(event_timestamp)) AS weekday,
         EXTRACT(YEAR FROM TIMESTAMP_MICROS(event_timestamp)) AS event_year,  -- Extract year
         campaign as camp
  FROM `tc-da-1.turing_data_analytics.raw_events`
),
Sessions AS (
  SELECT *,
         CASE 
              WHEN TIMESTAMP_DIFF(TIMESTAMP_MICROS(event_timestamp), TIMESTAMP_MICROS(last_event), SECOND) > 1800 OR last_event IS NULL 
              THEN 1 
              ELSE 0 
         END AS is_new_session
  FROM EventsWithLastEvent
  WHERE NOT (event_year = 2021 AND campaign IN ('BlackFriday_V1', 'Holiday_V1'))  -- Exclude outliers
),
-- Step 2: Mapping every event to its session
SessionMap AS (
  SELECT *,
         SUM(is_new_session) OVER (PARTITION BY user_pseudo_id, weekday, campaign ORDER BY event_timestamp) AS global_session_id
  FROM Sessions
),
-- Step 3: Calculate the length of each session
SessionDurations AS (
  SELECT 
    weekday,
    campaign,
    global_session_id,
    TIMESTAMP_DIFF(MAX(TIMESTAMP_MICROS(event_timestamp)), MIN(TIMESTAMP_MICROS(event_timestamp)), SECOND) / 60.0 AS session_duration_minutes
  FROM SessionMap
  GROUP BY weekday, campaign, global_session_id
)
-- Step 4: Pivot Table by Weekday
SELECT 
    CASE 
    WHEN weekday = 1 THEN 'Sunday'
    WHEN weekday = 2 THEN 'Monday'
    WHEN weekday = 3 THEN 'Tuesday'
    WHEN weekday = 4 THEN 'Wednesday'
    WHEN weekday = 5 THEN 'Thursday'
    WHEN weekday = 6 THEN 'Friday'
    WHEN weekday = 7 THEN 'Saturday'
  END AS weekday_name,
  COUNT(global_session_id) AS num_sessions,
  SUM(session_duration_minutes) AS total_session_duration_minutes,
  AVG(session_duration_minutes) AS avg_session_duration_minutes,
  AVG(CASE WHEN campaign = 'NewYear_V1' THEN session_duration_minutes ELSE 0 END) AS NewYear_V1_Duration,
  AVG(CASE WHEN campaign = 'NewYear_V2' THEN session_duration_minutes ELSE 0 END) AS NewYear_V2_Duration,
  AVG(CASE WHEN campaign = 'Holiday_V1' THEN session_duration_minutes ELSE 0 END) AS Holiday_V1_Duration,
  AVG(CASE WHEN campaign = 'Holiday_V2' THEN session_duration_minutes ELSE 0 END) AS Holiday_V2_Duration,
  AVG(CASE WHEN campaign = 'BlackFriday_V1' THEN session_duration_minutes ELSE 0 END) AS BlackFriday_V1_Duration,
  AVG(CASE WHEN campaign = 'BlackFriday_V2' THEN session_duration_minutes ELSE 0 END) AS BlackFriday_V2_Duration
  -- Add more campaigns as needed
FROM SessionDurations
Where session_duration_minutes >= 0.05
GROUP BY weekday
ORDER BY weekday;
