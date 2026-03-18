with atbs_next_raw as (
  SELECT DISTINCT
    AccountNumber_RPID,
    brand,
    timestamp,
    ProductSKU AS itemnumber,
    date
  FROM warehouse.bq_atbs_next_uk
  INNER JOIN warehouse.bq_sessions_next_uk USING (UniqueVisitID, DATE)
  WHERE date between date"{start_date}" and date"{end_date}"
),  
atbs_app_raw as (
  SELECT DISTINCT
    AccountNumber_RPID,
    ProductSKU as itemnumber,
    timestamp,
    date
  FROM warehouse.bq_atbs_next_uk_app
  INNER JOIN warehouse.bq_sessions_next_uk_app USING (UniqueVisitID, DATE)
  WHERE date between date"{start_date}" and date"{end_date}"
),
atbs_next as (
  SELECT
    AccountNumber_RPID,
    itemnumber,
    "web" as type,
    date,
    max(timestamp) as timestamp
  FROM atbs_next_raw
  WHERE itemnumber is not null
    AND AccountNumber_RPID is not null
  GROUP BY 1,2,3,4
),
atbs_app as (
  SELECT
    AccountNumber_RPID,
    itemnumber,
    "app" as type,
    date,
    max(timestamp) as timestamp
  FROM atbs_app_raw
  WHERE itemnumber is not null
    AND AccountNumber_RPID is not null
  GROUP BY 1,2,3,4
)
SELECT *, current_date() as rundate
FROM atbs_next
UNION DISTINCT
  (SELECT *, current_date() as rundate FROM atbs_app)
