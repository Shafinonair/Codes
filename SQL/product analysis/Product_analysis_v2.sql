WITH DailyUserEvents AS (
  SELECT 
    user_pseudo_id,
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date,
    MIN(CASE WHEN event_name = 'session_start' THEN TIMESTAMP_MICROS(event_timestamp) END) AS first_session_time,
    MIN(CASE WHEN event_name = 'purchase' THEN TIMESTAMP_MICROS(event_timestamp) END) AS first_purchase_time
  FROM 
    `tc-da-1.turing_data_analytics.raw_events`
  WHERE 
    event_name IN ('session_start', 'purchase')
    AND (event_name != 'purchase' OR purchase_revenue_in_usd IS NOT NULL)
  GROUP BY 
    user_pseudo_id, event_date
),
TimeToPurchase AS (
  SELECT 
    user_pseudo_id,
    event_date,
    TIMESTAMP_DIFF(first_purchase_time, first_session_time, SECOND) AS time_to_purchase_seconds
  FROM 
    DailyUserEvents
  WHERE 
    first_purchase_time IS NOT NULL
)
SELECT 
  event_date,
  AVG(time_to_purchase_seconds) AS avg_time_to_purchase,
  -- Add other statistical measures here
FROM 
  TimeToPurchase
GROUP BY 
  event_date
ORDER BY 
  event_date;
