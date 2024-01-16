  #How was the e-commerce sales? Did they grow up OVER time?
SELECT
  EXTRACT(YEAR
  FROM
    order_purchase_timestamp) AS order_purchase_year,
  FORMAT_DATE('%Y%m', order_purchase_timestamp) AS order_purchase_year_month,
  COUNT(orders_data.order_id) AS total_orders,
  ROUND(SUM(order_item.price)) AS Price,
  ROUND(SUM(order_item.freight_value)) AS freight_value,
  ROUND(SUM(order_item.price)) / COUNT(orders_data.order_id) AS price_per_order,
  ROUND(SUM(order_item.freight_value) / COUNT(orders_data.order_id)) AS freight_per_order,
FROM
  `tc-da-1.olist_db.olist_orders_dataset` AS orders_data
JOIN
  tc-da-1.olist_db.olist_order_items_dataset AS order_item
ON
  orders_data.order_id = order_item.order_id
WHERE
  order_purchase_timestamp >= '2017-01-01'
GROUP BY
  1,
  2;


  
SELECT
  EXTRACT(YEAR
  FROM
    order_purchase_timestamp) AS order_purchase_year,
  DATE_TRUNC(order_purchase_timestamp, MONTH) AS first_day_of_month,
  COUNT(orders_data.order_id) AS total_orders,
  ROUND(SUM(order_item.price)) AS Price,
  ROUND(SUM(order_item.freight_value)) AS freight_value,
  ROUND(SUM(order_item.price) / COUNT(orders_data.order_id)) AS price_per_order,
  ROUND(SUM(order_item.freight_value) / COUNT(orders_data.order_id)) AS freight_per_order
FROM
  `tc-da-1.olist_db.olist_orders_dataset` AS orders_data
JOIN
  tc-da-1.olist_db.olist_order_items_dataset AS order_item
ON
  orders_data.order_id = order_item.order_id
WHERE
  order_purchase_timestamp between '2017-01-01' and '2018-09-01'
GROUP BY
  1,
  2
ORDER BY
  first_day_of_month;