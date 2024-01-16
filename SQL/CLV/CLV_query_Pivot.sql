WITH RegistrationCohorts AS (
    -- Getting the registration week for each user
    SELECT 
        user_pseudo_id,
        MIN(DATE_TRUNC(TIMESTAMP_MICROS(event_timestamp), WEEK)) AS registration_week
    FROM 
        turing_data_analytics.raw_events 
    GROUP BY 
        user_pseudo_id
),

WeeklyRevenue AS (
    -- Getting the revenue for each user for each week, considering even those who didn't purchase
    SELECT 
        user_pseudo_id, 
        COALESCE(SUM(purchase_revenue_in_usd), 0) AS total_revenue, 
        DATE_TRUNC(TIMESTAMP_MICROS(event_timestamp), WEEK) AS revenue_week
    FROM 
        turing_data_analytics.raw_events
    GROUP BY 
        user_pseudo_id, DATE_TRUNC(TIMESTAMP_MICROS(event_timestamp), WEEK)
),

JoinedData AS (
    SELECT 
        DATE(rc.registration_week) AS cohort_week, 
        rc.user_pseudo_id AS id,
        wr.total_revenue,
        DATE_DIFF(DATE(wr.revenue_week), DATE(rc.registration_week), WEEK) AS week_nr
    FROM 
        RegistrationCohorts rc
    LEFT JOIN 
        WeeklyRevenue wr ON rc.user_pseudo_id = wr.user_pseudo_id
    WHERE 
        DATE(wr.revenue_week) >= DATE(rc.registration_week)
)

-- Pivot the data
SELECT 
    cohort_week,
    COUNT(DISTINCT id) AS total_users,
    SUM(CASE WHEN week_nr = 0 THEN total_revenue ELSE 0 END) / COUNT(DISTINCT id) AS week_0,
    SUM(CASE WHEN week_nr = 1 THEN total_revenue ELSE 0 END) / COUNT(DISTINCT id) AS week_1,
    SUM(CASE WHEN week_nr = 2 THEN total_revenue ELSE 0 END) / COUNT(DISTINCT id) AS week_2,
    SUM(CASE WHEN week_nr = 3 THEN total_revenue ELSE 0 END) / COUNT(DISTINCT id) AS week_3,
    SUM(CASE WHEN week_nr = 4 THEN total_revenue ELSE 0 END) / COUNT(DISTINCT id) AS week_4,
    SUM(CASE WHEN week_nr = 5 THEN total_revenue ELSE 0 END) / COUNT(DISTINCT id) AS week_5,
    SUM(CASE WHEN week_nr = 6 THEN total_revenue ELSE 0 END) / COUNT(DISTINCT id) AS week_6,
    SUM(CASE WHEN week_nr = 7 THEN total_revenue ELSE 0 END) / COUNT(DISTINCT id) AS week_7,
    SUM(CASE WHEN week_nr = 8 THEN total_revenue ELSE 0 END) / COUNT(DISTINCT id) AS week_8,
    SUM(CASE WHEN week_nr = 9 THEN total_revenue ELSE 0 END) / COUNT(DISTINCT id) AS week_9,
    SUM(CASE WHEN week_nr = 10 THEN total_revenue ELSE 0 END) / COUNT(DISTINCT id) AS week_10,
    SUM(CASE WHEN week_nr = 11 THEN total_revenue ELSE 0 END) / COUNT(DISTINCT id) AS week_11,
    SUM(CASE WHEN week_nr = 12 THEN total_revenue ELSE 0 END) / COUNT(DISTINCT id) AS week_12,

    -- Continue the pattern for weeks 1 through 12

FROM 
    JoinedData
WHERE 
    cohort_week <= '2021-01-24' -- Only consider cohorts up to the week ending 2021-01-24
GROUP BY 
    cohort_week
ORDER BY 
    cohort_week;
