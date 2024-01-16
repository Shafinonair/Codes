#distribution of payment by payment method

SELECT
  payment_type,
  COUNT(*) as total_transactions,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM `tc-da-1.olist_db.olist_order_payments_dataset`), 2) as percentage
FROM
  `tc-da-1.olist_db.olist_order_payments_dataset`
GROUP BY
  payment_type
ORDER BY
  total_transactions DESC;

#distribution of payment by installment

WITH installment_data AS (
  SELECT
    payment_installments,
    COUNT(1) AS installment_count
  FROM
    `tc-da-1.olist_db.olist_order_payments_dataset`
  GROUP BY
    payment_installments
),
total_data AS (
  SELECT
    SUM(installment_count) AS total_count
  FROM
    installment_data
)

SELECT
  id.payment_installments,
  id.installment_count,
  ROUND(id.installment_count * 100.0 / td.total_count, 2) AS percentage
FROM
  installment_data id
CROSS JOIN
  total_data td
ORDER BY
  id.payment_installments;

#payment type timeseries data
SELECT
  DATE_TRUNC(DATE(o.order_purchase_timestamp), MONTH) AS first_day_of_month,
  p.payment_type,
  COUNT(*) as total_transactions,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM `tc-da-1.olist_db.olist_order_payments_dataset`), 2) as percentage
FROM
  `tc-da-1.olist_db.olist_order_payments_dataset` p
JOIN
  `tc-da-1.olist_db.olist_orders_dataset` o ON p.order_id = o.order_id
Where 
    o.order_purchase_timestamp between '2017-01-01' and '2018-09-01'
GROUP BY
  first_day_of_month,
  p.payment_type

ORDER BY
  first_day_of_month,
  total_transactions DESC;

#customer state and payment

SELECT
  DATE_TRUNC(DATE(o.order_purchase_timestamp), MONTH) AS first_day_of_month,
  c.customer_state,
  p.payment_type,
  COUNT(p.payment_type) AS total_payments,
  ROUND(SUM(p.payment_value), 2) AS total_payment_value
FROM
  `tc-da-1.olist_db.olist_order_payments_dataset` p
JOIN
  `tc-da-1.olist_db.olist_orders_dataset` o ON p.order_id = o.order_id
JOIN
  `tc-da-1.olist_db.olist_customesr_dataset` c ON o.customer_id = c.customer_id

Where 
    o.order_purchase_timestamp between '2017-01-01' and '2018-09-01'
GROUP BY
  first_day_of_month,
  c.customer_state,
  p.payment_type
ORDER BY
  first_day_of_month,
  c.customer_state,
  total_payments DESC;

#order status by payment value

SELECT 
order_status,
payment_type,
Avg(payment_value) As Average_payment_value

 FROM `tc-da-1.olist_db.olist_orders_dataset` AS  order_data
 JOIN `olist_db.olist_order_payments_dataset`  AS  payment_data
 on  order_data.order_id = payment_data.order_id

 Where order_data.order_status = 'canceled'  
 Group by order_status, payment_type;

#avg installment by order status

SELECT 
order_status,
payment_type,
Avg(payment_installments) As Average_payment_installment

 FROM `tc-da-1.olist_db.olist_orders_dataset` AS  order_data
 JOIN `olist_db.olist_order_payments_dataset`  AS  payment_data
 on  order_data.order_id = payment_data.order_id
  Where order_data.order_status = 'canceled'
 Group by order_status, payment_type;
