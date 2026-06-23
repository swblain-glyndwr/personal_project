SELECT DISTINCT
  CASE
    WHEN
      LENGTH(pid) > 6
      AND LOWER(next_category) = 'sofas'
    THEN
      SUBSTRING_INDEX(sku_id, '_', -1)
    WHEN
      LENGTH(pid) > 6
      AND (
        CHARINDEX('suit', LOWER(next_category)) > 0
        OR CHARINDEX('waistcoat', LOWER(next_category)) > 0
        OR CHARINDEX('jacket', LOWER(next_category)) > 0
        OR CHARINDEX('bracelets|tshirts', LOWER(next_category)) > 0
        OR CHARINDEX('bras|towels', LOWER(next_category)) > 0
        OR CHARINDEX('dresses|jeans', LOWER(next_category)) > 0
        OR CHARINDEX('shorts|tshirts', LOWER(next_category)) > 0
        OR CHARINDEX('dresses', LOWER(next_category)) > 0
      )
    THEN
      SUBSTRING(SUBSTRING_INDEX(sku_id, '_', -2), 2, 6)
    ELSE pid
  END AS pid,
  department
FROM
  businessintelligencesystems_prod.ecommerce.bloomreach_uk_product_catalog -- partitioned by FileDateTime	timestamp
WHERE
  to_date(FileDateTime) > current_date - 730