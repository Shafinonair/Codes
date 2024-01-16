SELECT
  product.ProductId,
  product.Name,
  product.ProductNumber,
  product.size,
  product.color,
  product.ProductSubcategoryId,
  product_sub.Name AS Subcategory
FROM
  adwentureworks_db.productsubcategory AS product_sub
JOIN
  adwentureworks_db.product AS product
ON
  product_sub.ProductSubcategoryId = product.ProductSubcategoryId
ORDER BY
  product_sub.Name