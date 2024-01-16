WITH 
-- Step 1: Compute for F & M for the given date range:
t1 AS (
    SELECT  
        CustomerID,
        Country,
        MAX(InvoiceDate) AS last_purchase_date,
        COUNT(DISTINCT InvoiceNo) AS frequency,
        SUM(UnitPrice * Quantity ) AS monetary 
    FROM `tc-da-1.turing_data_analytics.rfm`
    WHERE CustomerID IS NOT NULL AND InvoiceDate BETWEEN '2010-12-01' AND '2011-12-01'
    GROUP BY CustomerID, Country 
),

-- Step 2: Compute for R using the given reference date:
t2 AS (
    SELECT *,
        DATE_DIFF(DATE '2011-12-01', DATE(last_purchase_date), DAY) AS recency
    FROM t1
),

-- Step 3: Calculate R, F, M scores using quartiles:
t3 AS (
    SELECT 
        a.*,
        b.percentiles[offset(25)] AS m25, 
        b.percentiles[offset(50)] AS m50,
        b.percentiles[offset(75)] AS m75,
        c.percentiles[offset(25)] AS f25, 
        c.percentiles[offset(50)] AS f50,
        c.percentiles[offset(75)] AS f75,
        d.percentiles[offset(25)] AS r25, 
        d.percentiles[offset(50)] AS r50,
        d.percentiles[offset(75)] AS r75
    FROM 
        t2 a,
        (SELECT APPROX_QUANTILES(monetary, 100) percentiles FROM t2) b,
        (SELECT APPROX_QUANTILES(frequency, 100) percentiles FROM t2) c,
        (SELECT APPROX_QUANTILES(recency, 100) percentiles FROM t2) d
),

t4 AS (
    SELECT *, 
        CAST(ROUND((f_score + m_score) / 2, 0) AS INT64) AS fm_score
    FROM (
        SELECT *, 
            CASE WHEN monetary <= m25 THEN 1
                 WHEN monetary <= m50 THEN 2 
                 WHEN monetary <= m75 THEN 3 
                 ELSE 4
            END AS m_score,
            CASE WHEN frequency <= f25 THEN 1
                 WHEN frequency <= f50 THEN 2 
                 WHEN frequency <= f75 THEN 3 
                 ELSE 4
            END AS f_score,
            CASE WHEN recency <= r25 THEN 4
                 WHEN recency <= r50 THEN 3 
                 WHEN recency <= r75 THEN 2 
                 ELSE 1
            END AS r_score
        FROM t3
    )
),

-- Step 4 & 5: Assign an overall RFM score and segment customers:
t5 AS (
    SELECT 
        CustomerID, 
        Country,
        recency,
        frequency, 
        monetary,
        r_score,
        f_score,
        m_score,
        fm_score,
        CASE 
            WHEN r_score = 4 AND fm_score = 4 THEN 'Best Customers'
            WHEN r_score = 4 AND (fm_score = 3 OR fm_score = 2) THEN 'Loyal Customers'
            WHEN r_score = 3 AND (fm_score = 4 OR fm_score = 3) THEN 'Big Spenders'
            WHEN r_score = 4 AND fm_score = 1 THEN 'Recent Customers'
            WHEN (r_score = 3 AND fm_score = 2) OR (r_score = 2 AND fm_score = 4) THEN 'Promising Customers'
            WHEN r_score = 3 AND fm_score = 1 THEN 'Customers Needing Attention'
            WHEN (r_score = 2 AND fm_score = 3) OR (r_score = 2 AND fm_score = 2) THEN 'About to Sleep'
            WHEN r_score = 2 AND fm_score = 1 THEN 'At Risk'
            WHEN r_score = 1 AND (fm_score = 4 OR fm_score = 3) THEN 'Cant Lose Them'
            WHEN r_score = 1 AND fm_score = 2 THEN 'Hibernating'
            WHEN r_score = 1 AND fm_score = 1 THEN 'Lost'
        END AS rfm_segment 
    FROM t4
)

SELECT * FROM t5
