WITH SortedEvents AS (
  SELECT 
    user_pseudo_id,
    event_name,
    TIMESTAMP_MICROS(event_timestamp) AS event_datetime,
    purchase_revenue_in_usd
  FROM 
    `tc-da-1.turing_data_analytics.raw_events`
  WHERE 
    event_name IN ('session_start', 'purchase')
    AND (event_name != 'purchase' OR purchase_revenue_in_usd IS NOT NULL)
  ORDER BY 
    user_pseudo_id, event_datetime
)
, TimeToPurchase AS (
  SELECT 
    user_pseudo_id,
    MIN(CASE WHEN event_name = 'purchase' THEN event_datetime END) AS first_purchase_time,
    MIN(CASE WHEN event_name = 'session_start' THEN event_datetime END) AS first_session_time
  FROM 
    SortedEvents
  GROUP BY 
    user_pseudo_id
)
SELECT 
  user_pseudo_id,
  TIMESTAMP_DIFF(first_purchase_time, first_session_time, SECOND) AS time_to_purchase_seconds
FROM 
  TimeToPurchase
WHERE 
  first_purchase_time IS NOT NULL;
