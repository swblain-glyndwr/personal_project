-- Databricks notebook source

-- CREATE WIDGETS
CREATE WIDGET TEXT catalog_schema_prefix DEFAULT 'marketingdata_dev.ds_sandbox';
CREATE WIDGET TEXT table_prefix DEFAULT 'next_uk_nextAds_analytics_pctr';
CREATE WIDGET TEXT start_date DEFAULT '2026-06-01';
CREATE WIDGET TEXT end_date DEFAULT '2026-06-01';
CREATE WIDGET TEXT lookback_period DEFAULT '30';
CREATE WIDGET TEXT year_lookback_period DEFAULT '365';


SET spark.sql.adaptive.enabled = true;
SET spark.sql.execution.arrow.pyspark.enabled = true;

/* Sessions base */
CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_sessions') AS
(
  WITH cte_sessions AS (
    SELECT
      s.device,
      CASE
        WHEN s.device = 'Mobile' THEN 'Mobile'
        WHEN s.device = 'Desktop' THEN 'Desktop'
        ELSE 'Other'
      END as device_simple,
      s.geocountry,
      CASE
        WHEN
          s.geocountry IN ('United Kingdom', 'Ireland', 'Jersey', 'Isle of Man', 'Guernsey')
        THEN
          'UK & Ireland'
        WHEN m.segment_name IS NOT NULL THEN m.segment_name
        ELSE 'Other'
      END AS geocountry_simple,
      s.channel,
      CASE
        WHEN
          s.channel IN (
            'Paid Search',
            'Organic Search',
            'Paid Social',
            'Organic Social',
            'Direct',
            'Email',
            'Referral',
            'SMS'
          )
        THEN
          s.channel
        WHEN s.channel regexp '^.*(Paid).*$' THEN 'Paid Other'
        WHEN s.channel regexp '^.*(Organic).*$' THEN 'Organic Other'
        ELSE 'Other'
      END AS channel_simple,
      c.account_number,
      s.UniqueVisitID,
      s.Timeonsite_seconds,
      c.account_number IS NOT NULL AS customer_filter,
      c.gender,
      RANK() OVER (PARTITION BY s.UniqueVisitID ORDER BY s.DATE ASC) AS session_rank,
      RANK() OVER (
          PARTITION BY c.account_number
          ORDER BY
            s.date DESC,
            s.visitstarthour DESC,
            -- Tiebreakers - if multiple sessions on same day/ hour
            s.UniqueVisitID DESC
        ) AS session_order,
      Dayofweek(s.date) AS session_dow,
      s.date
    FROM
      marketingdata_prod.warehouse.bq_sessions_next_uk AS s
        -- Want all sessions regardless of if we can match customer
        -- Important for getting CTR for device types etc
        LEFT JOIN marketingdata_prod.warehouse.rpid_with_accounts AS rpid
          ON rpid.roamingprofileid = s.RPID
        LEFT JOIN marketingdata_prod.warehouse.svoccust AS c
          ON c.account_number = rpid.account_number
          AND c.countrycode = 'GB'
          AND c.client = 'NEXT'
        LEFT JOIN marketingdata_prod.search.nov_country_mapping AS m
          ON m.country_name = s.geocountry
    WHERE
      -- Offset to a day prior to rundate
      s.date BETWEEN
        :start_date - (INTERVAL '1 DAY' * (:lookback_period + 1))
      AND
        :end_date - INTERVAL '1' DAY
  ),
   cte_app_sessions AS (
    SELECT
      COALESCE(s.Device , 'App') AS device,
      CASE
        WHEN s.device = 'Mobile' THEN 'Mobile'
        WHEN s.device = 'Desktop' THEN 'Desktop'
        WHEN s.device= 'App' THEN 'App'
        ELSE 'Other'
      END as device_simple,
      s.geocountry,
      CASE
        WHEN
          s.geocountry IN ('United Kingdom', 'Ireland', 'Jersey', 'Isle of Man', 'Guernsey')
        THEN
          'UK & Ireland'
        WHEN m.segment_name IS NOT NULL THEN m.segment_name
        ELSE 'Other'
      END AS geocountry_simple,
      s.channel,
      CASE
        WHEN
          s.channel IN (
            'Paid Search',
            'Organic Search',
            'Paid Social',
            'Organic Social',
            'Direct',
            'Email',
            'Referral',
            'SMS'
          )
        THEN
          s.channel
        WHEN s.channel regexp '^.*(Paid).*$' THEN 'Paid Other'
        WHEN s.channel regexp '^.*(Organic).*$' THEN 'Organic Other'
        ELSE 'Other'
      END AS channel_simple,
      c.account_number,
      s.UniqueVisitID,
      s.Timeonsite_seconds,
      c.account_number IS NOT NULL AS customer_filter,
      c.gender,
      RANK() OVER (PARTITION BY s.UniqueVisitID ORDER BY s.DATE ASC) AS session_rank,
      RANK() OVER (
          PARTITION BY c.account_number
          ORDER BY
            s.date DESC,
            s.visitstarthour DESC,
            -- Tiebreakers - if multiple sessions on same day/ hour
            s.UniqueVisitID DESC
        ) AS session_order,
      Dayofweek(s.date) AS session_dow,
      s.date
    FROM
      marketingdata_prod.warehouse.bq_sessions_next_uk_app AS s
        -- Want all sessions regardless of if we can match customer
        -- Important for getting CTR for device types etc
        LEFT JOIN marketingdata_prod.warehouse.rpid_with_accounts AS rpid
          ON rpid.roamingprofileid = s.RPID
        LEFT JOIN marketingdata_prod.warehouse.svoccust AS c
          ON c.account_number = rpid.account_number
          AND c.countrycode = 'GB'
          AND c.client = 'NEXT'
        LEFT JOIN marketingdata_prod.search.nov_country_mapping AS m
          ON m.country_name = s.geocountry
    WHERE
      -- Offset to a day prior to rundate
      s.date BETWEEN
        :start_date - (INTERVAL '1 DAY' * (:lookback_period + 1))
      AND
        :end_date - INTERVAL '1' DAY
  )
  SELECT
    *
  FROM
    cte_sessions
  WHERE
    session_rank = 1
  UNION 
  SELECT
    *
  FROM
    cte_app_sessions
  WHERE
    session_rank = 1
);



-- Primary key and constraints 
ALTER TABLE
  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_sessions')
ALTER COLUMN
  UniqueVisitID
  SET NOT NULL;

ALTER TABLE
  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_sessions')
ADD
  PRIMARY KEY (UniqueVisitID);


/* Advert Clicks and Impresssions base */

--CURRENTLY IS ONLY WEB SHOPPING BAG CLICKS

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_ad_clicks_impressions_base') AS (
SELECT 
      s.UniqueVisitID
    , s.account_number
    , a.date
    , a.timestamp
    , DAYOFWEEK(a.date) AS dow
    , weekofyear(a.date) AS woy 
    , s.geocountry 
    , s.channel
    , s.device 
    , s.gender
    , s.geocountry_simple
    , s.channel_simple
    , s.device_simple 
    , a.Level2 As AdvertID
    , split_part(a.Level2 , '_',1) AS pot 
    -- , split_part(a.Level2 , '_',2) AS campaign 
    -- Temporary logic to append the campaign suffixes
    , CASE WHEN lower(a.Level2) regexp '^.*(younger).*$' THEN split_part(a.Level2 , '_',2)||'_Y'
        WHEN lower(a.Level2) regexp '^.*(older).*$' THEN split_part(a.Level2 , '_',2)||'_O'
        WHEN lower(a.Level2) regexp '^.*(toddler).*$' THEN split_part(a.Level2 , '_',2)||'_T'
        WHEN lower(a.Level2) regexp '^.*(baby).*$' THEN split_part(a.Level2 , '_',2)||'_N'
        WHEN lower(a.Level2) regexp '^.*(teen).*$' THEN split_part(a.Level2 , '_',2)||'_TE'
        ELSE split_part(a.Level2 , '_',2) END campaign 
    , REGEXP_EXTRACT(a.Level2, '^.*_(V[1-9])_.*$', 1) AS versionnumber
    , a.PagePath 
    , a.action 
FROM 
    marketingdata_prod.warehouse.bq_actions_next_uk AS a
    INNER JOIN IDENTIFIER (:catalog_schema_prefix||'.'|| :table_prefix || '_sessions') AS s
        ON s.UniqueVisitID = a.UniqueVisitID
        AND a.action IN ('Banner Impression - Next Ads', 'Banner Click - Next Ads')
        -- Currently filtered for shopping bag 
        AND a.PagePath ='/shoppingbag'
        --Filtered to records where we have an AdvertId 
        AND a.Level2 regexp "^P"
    WHERE a.date BETWEEN
        :start_date - (INTERVAL '1 DAY' * (:lookback_period + 1))
      AND
        :end_date - INTERVAL '1' DAY
);

