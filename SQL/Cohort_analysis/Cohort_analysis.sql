WITH
  date_range AS(
  SELECT
    user_pseudo_id,
    MIN(DATE_TRUNC(subscription_start,WEEK)) AS start_date,
    MAX(DATE_TRUNC(subscription_end,WEEK)) AS end_date
  FROM
    turing_data_analytics.subscriptions
  GROUP BY
    user_pseudo_id
  ORDER BY
    start_date )

SELECT
  start_date,
  SUM(CASE
      WHEN date_range.start_date IS NOT NULL THEN 1
    ELSE
    0
  END
    ) AS cohort_size,
  SUM(CASE
      WHEN date_range.end_date = date_range.start_date OR date_range.end_date IS NULL THEN 1
    ELSE
    0
  END
    ) AS week_0,

  SUM(CASE
      WHEN date_range.end_date > DATE_ADD(date_range.start_date,INTERVAL 1 week) OR date_range.end_date IS NULL THEN 1
    ELSE
    0
  END
    ) AS week_1,

   SUM(CASE
      WHEN date_range.end_date > DATE_ADD(date_range.start_date,INTERVAL 2 week) OR date_range.end_date IS NULL THEN 1
    ELSE
    0
  END
    ) AS week_2,

     SUM(CASE
      WHEN date_range.end_date > DATE_ADD(date_range.start_date,INTERVAL 3 week) OR date_range.end_date IS NULL THEN 1
    ELSE
    0
  END
    ) AS week_3,

     SUM(CASE
      WHEN date_range.end_date > DATE_ADD(date_range.start_date,INTERVAL 4 week) OR date_range.end_date IS NULL THEN 1
    ELSE
    0
  END
    ) AS week_4,

     SUM(CASE
      WHEN date_range.end_date > DATE_ADD(date_range.start_date,INTERVAL 5 week) OR date_range.end_date IS NULL THEN 1
    ELSE
    0
  END
    ) AS week_5,

     SUM(CASE
      WHEN date_range.end_date > DATE_ADD(date_range.start_date,INTERVAL 6 week) OR date_range.end_date IS NULL THEN 1
    ELSE
    0
  END
    ) AS week_6,

     SUM(CASE
      WHEN date_range.end_date > DATE_ADD(date_range.start_date,INTERVAL 7 week) OR date_range.end_date IS NULL THEN 1
    ELSE
    0
  END
    ) AS week_7,

     SUM(CASE
      WHEN date_range.end_date > DATE_ADD(date_range.start_date,INTERVAL 8 week) OR date_range.end_date IS NULL THEN 1
    ELSE
    0
  END
    ) AS week_8,

     SUM(CASE
      WHEN date_range.end_date > DATE_ADD(date_range.start_date,INTERVAL 9 week) OR date_range.end_date IS NULL THEN 1
    ELSE
    0
  END
    ) AS week_9,

     SUM(CASE
      WHEN date_range.end_date > DATE_ADD(date_range.start_date,INTERVAL 10 week) OR date_range.end_date IS NULL THEN 1
    ELSE
    0
  END
    ) AS week_10,

     SUM(CASE
      WHEN date_range.end_date > DATE_ADD(date_range.start_date,INTERVAL 11 week) OR date_range.end_date IS NULL THEN 1
    ELSE
    0
  END
    ) AS week_11,

     SUM(CASE
      WHEN date_range.end_date > DATE_ADD(date_range.start_date,INTERVAL 12 week) OR date_range.end_date IS NULL THEN 1
    ELSE
    0
  END
    ) AS week_12,

     SUM(CASE
      WHEN date_range.end_date > DATE_ADD(date_range.start_date,INTERVAL 13 week) OR date_range.end_date IS NULL THEN 1
    ELSE
    0
  END
    ) AS week_13,

     SUM(CASE
      WHEN date_range.end_date > DATE_ADD(date_range.start_date,INTERVAL 14 week) OR date_range.end_date IS NULL THEN 1
    ELSE
    0
  END
    ) AS week_14

FROM
  date_range
GROUP BY
  date_range.start_date