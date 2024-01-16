SELECT
  DATE_TRUNC(DATE(o.order_purchase_timestamp), MONTH) AS first_day_of_month,
  Sum(p.payment_value) As total_sales_ammount,
  
FROM
  `tc-da-1.olist_db.olist_order_payments_dataset` p
JOIN
  `tc-da-1.olist_db.olist_orders_dataset` o ON p.order_id = o.order_id
Where 
    o.order_purchase_timestamp between '2017-01-01' and '2018-09-01'
GROUP BY
  first_day_of_month
ORDER BY
  first_day_of_month ASC;