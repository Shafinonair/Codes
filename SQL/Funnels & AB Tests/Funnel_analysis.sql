WITH  
  deduplicated_events AS (
    SELECT
      user_pseudo_id,
      country,
      event_name,
      event_timestamp,
      ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) AS row_number
    FROM
      `tc-da-1.turing_data_analytics.raw_events`
    WHERE
      event_name IN (
        'first_visit',
        'view_promotion',
        'view_item',
        'add_to_cart',
        'begin_checkout',
        'purchase'
      )
  ),
  unique_data AS (
    SELECT
      country,
      event_name,
      COUNT( user_pseudo_id) AS user_count
    FROM
      deduplicated_events
    WHERE
      row_number = 1
    GROUP BY
      country,
      event_name
  ),
  
  funnel_summary AS (
    SELECT
      country,
      event_name,
      user_count,
      ROW_NUMBER() OVER (PARTITION BY event_name ORDER BY user_count DESC) AS country_rank
    FROM
      unique_data
  )
SELECT
  event_name,
  SUM(CASE WHEN country_rank = 1 THEN user_count END) AS top_country_1,
  SUM(CASE WHEN country_rank = 2 THEN user_count END) AS top_country_2,
  SUM(CASE WHEN country_rank = 3 THEN user_count END) AS top_country_3
FROM
  funnel_summary
GROUP BY
  event_name
ORDER BY
  event_name
