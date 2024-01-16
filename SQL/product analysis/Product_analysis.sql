
-- SQL query to find the most frequently visited pages before a purchase is made
WITH PurchaseEvents AS (
    SELECT user_pseudo_id, MIN(event_timestamp) AS first_purchase_time
    FROM `tc-da-1.turing_data_analytics.raw_events`
    WHERE event_name = 'purchase'
    GROUP BY user_pseudo_id
),
VisitedPagesBeforePurchase AS (
    SELECT e.page_title, COUNT(*) AS visits_before_purchase
    FROM `tc-da-1.turing_data_analytics.raw_events` e
    JOIN PurchaseEvents p ON e.user_pseudo_id = p.user_pseudo_id
    WHERE e.event_timestamp < p.first_purchase_time AND e.event_name = 'page_view'
    GROUP BY e.page_title
)
SELECT page_title, visits_before_purchase
FROM VisitedPagesBeforePurchase
ORDER BY visits_before_purchase DESC;

-- SQL query to find the most frequent exit pages

WITH LastEventPerUser AS (
    SELECT user_pseudo_id, MAX(event_timestamp) AS last_event_time
    FROM `tc-da-1.turing_data_analytics.raw_events`
    GROUP BY user_pseudo_id
),
ExitPages AS (
    SELECT e.page_title, COUNT(*) AS total_exits
    FROM `tc-da-1.turing_data_analytics.raw_events` e
    JOIN LastEventPerUser le ON e.user_pseudo_id = le.user_pseudo_id
    WHERE e.event_timestamp = le.last_event_time AND e.event_name = 'page_view'
    GROUP BY e.page_title
)
SELECT page_title, total_exits
FROM ExitPages
ORDER BY total_exits DESC;

-- SQL query to find the most frequent landing pages
WITH FirstEventPerUser AS (
    SELECT user_pseudo_id, MIN(event_timestamp) AS first_event_time
    FROM `tc-da-1.turing_data_analytics.raw_events`
    GROUP BY user_pseudo_id
),
LandingPages AS (
    SELECT e.page_title, COUNT(*) AS total_landings
    FROM `tc-da-1.turing_data_analytics.raw_events` e
    JOIN FirstEventPerUser fe ON e.user_pseudo_id = fe.user_pseudo_id
    WHERE e.event_timestamp = fe.first_event_time AND e.event_name = 'page_view'
    GROUP BY e.page_title
)
SELECT page_title, total_landings
FROM LandingPages
ORDER BY total_landings DESC;


-- SQL query to find device preferences for different actions
SELECT 
    event_name,
    category,
    COUNT (*) AS total_events
FROM `tc-da-1.turing_data_analytics.raw_events`
WHERE event_name = 'purchase'
GROUP BY event_name, category
ORDER BY event_name, total_events DESC;






