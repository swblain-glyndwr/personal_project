with views_next as (
  SELECT
    AccountNumber_RPID,
    ProductSKU AS itemnumber,
    "web" as type,
    date,
    max(timestamp) as timestamp
  FROM warehouse.bq_views_next_uk
  INNER JOIN warehouse.bq_sessions_next_uk USING (UniqueVisitID, DATE)
  WHERE date between date"{start_date}" and date"{end_date}"
    AND EventType regexp "pdp_view"
    AND ProductSKU is not null
    AND AccountNumber_RPID is not null
  GROUP BY 1,2,3,4
),
views_app as (
  SELECT
    AccountNumber_RPID,
    ProductSKU AS itemnumber,
    "app" as type,
    date,
    max(timestamp) as timestamp
  FROM warehouse.bq_views_next_uk_app
  INNER JOIN warehouse.bq_sessions_next_uk_app USING (UniqueVisitID, DATE)
  WHERE date between date"{start_date}" and date"{end_date}"
    -- AND ScreenName = "PDP" -- TODO: is this the best (least noisy) way to capture app views?
    AND ProductSKU is not null
    AND AccountNumber_RPID is not null
  GROUP BY 1,2,3,4
)
SELECT *, current_date() as rundate
FROM views_next
UNION ALL
SELECT *, current_date() as rundate
FROM views_app
