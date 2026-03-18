with views_next_raw as (
  SELECT DISTINCT
    AccountNumber_RPID,
    brand,
    timestamp,
    ProductSKU AS itemnumber,
    date
  FROM warehouse.bq_views_next_uk
  INNER JOIN warehouse.bq_sessions_next_uk USING (UniqueVisitID, DATE)
  WHERE date between date"{start_date}" and date"{end_date}"
    AND EventType regexp "pdp_view"
),
views_app_raw as (
  SELECT DISTINCT
    AccountNumber_RPID,
    ProductSKU as itemnumber,
    timestamp,
    date
  FROM warehouse.bq_views_next_uk_app
  INNER JOIN warehouse.bq_sessions_next_uk_app USING (UniqueVisitID, DATE)
  WHERE date between date"{start_date}" and date"{end_date}"
    -- AND ScreenName = "PDP" -- TODO: is this the best (least noisy) way to capture app views?
),
views_next as (
  SELECT
    AccountNumber_RPID,
    itemnumber,
    "web" as type,
    date,
    max(timestamp) as timestamp
  FROM views_next_raw
  WHERE itemnumber is not null
    AND AccountNumber_RPID is not null
  GROUP BY 1,2,3,4
),
views_app as (
  SELECT
    AccountNumber_RPID,
    itemnumber,
    "app" as type,
    date,
    max(timestamp) as timestamp
  FROM views_app_raw
  WHERE itemnumber is not null
    AND AccountNumber_RPID is not null
  GROUP BY 1,2,3,4
)

SELECT *
FROM views_next
UNION DISTINCT
  (SELECT * FROM views_app)