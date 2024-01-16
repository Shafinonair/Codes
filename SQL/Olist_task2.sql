#How the total sales are concentraded in brazilian states?

With Order_payment AS (
  SELECT * 

  FROM
    `tc-da-1.olist_db.olist_orders_dataset` AS orders_data
  JOIN
    tc-da-1.olist_db.olist_order_items_dataset AS order_item
  ON
    orders_data.order_id = order_item.order_id
  Where 
    order_purchase_timestamp between '2017-01-01' and '2018-09-01'
)

SELECT 
    customer_data.customer_state,
    Round(Sum(Order_payment.price)) As total_sales,
    Round(AVG(Order_payment.price)) As mean_price_by_state, --mean of price by customer state
    Round(Sum(Order_payment.freight_value)) As total_freight,
    Round(AVG(Order_payment.freight_value)) As mean_freight_by_state --mean of freight by customer state

FROM `tc-da-1.olist_db.olist_customesr_dataset` As customer_data

JOIN Order_payment
ON customer_data.customer_id = Order_payment.customer_id
Group by 1
