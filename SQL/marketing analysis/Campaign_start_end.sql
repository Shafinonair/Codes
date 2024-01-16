-- Calculate session counts grouped by user, date, year, and campaign
WITH SessionDurations AS (
  SELECT 
    user_pseudo_id,
    EXTRACT(YEAR FROM TIMESTAMP_MICROS(event_timestamp)) AS event_year,
    EXTRACT(DATE FROM TIMESTAMP_MICROS(event_timestamp)) AS event_date,
    campaign,
    COUNT(*) AS session_count
  FROM 
    `tc-da-1.turing_data_analytics.raw_events`
  GROUP BY 
    user_pseudo_id, event_date, event_year, campaign
),
-- Aggregate session metrics by day, year, and campaign
DailySessionMetrics AS (
  SELECT 
    event_year,
    event_date,
    campaign,
    SUM(session_count) AS total_sessions
  FROM 
    SessionDurations
  GROUP BY 
    event_date, event_year, campaign
  HAVING NOT (event_year = 2021 AND campaign IN ('BlackFriday_V1', 'Holiday_V1'))  -- Exclude outliers
)
-- Final query to get start and end dates for each campaign in each year and the total session duration
SELECT 
  event_year,
  campaign,
  MIN(event_date) AS Start_Date,
  MAX(event_date) AS End_Date,
FROM 
  DailySessionMetrics
GROUP BY 
  event_year, campaign
HAVING campaign IN ('NewYear_V1', 'NewYear_V2', 'Holiday_V1', 'Holiday_V2', 'BlackFriday_V1', 'BlackFriday_V2','(referral)','(organic)','(direct)')
ORDER BY
  event_year, campaign;
