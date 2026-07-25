-- Databricks notebook source

-- CREATE WIDGETS
CREATE WIDGET TEXT catalog_schema_prefix DEFAULT 'marketingdata_dev.ds_sandbox';
CREATE WIDGET TEXT table_prefix DEFAULT 'next_uk_nextAds_analytics_pctr';
CREATE WIDGET TEXT output_table_name DEFAULT '_features';


SET spark.sql.adaptive.enabled = true;
SET spark.sql.execution.arrow.pyspark.enabled = true;

/* Combine All features into output table */

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix ||'.'|| :table_prefix ||  :output_table_name) AS (
SELECT DISTINCT 
-- Identifiers 
  t.rundate
, t.account_number
, a.uniqueAdID
, a.potnumber
, a.campaignnumber
--Advert Seasonality 
, a.seasonal_flag
, a.number_impressions AS total_ad_impressions 
, a.percentage_impressions AS total_ad_perc_impressions
, a.cumulative_percentage_impressions AS total_ad_cumulative_perc_impressions
-- customer features 
, DATEDIFF(t.rundate - interval '1 day' ,t.accountstartdate) AS customer_lifespan
, t.age 
, t.gender
, t.mail_optout
, t.cash_acc
, t.staff_indicator
, t.postcodearea
-- Site session details
, COALESCE(sa.total_time_on_site,0) AS total_time_on_site
, COALESCE(sa.total_sessions,0) AS total_sessions
, COALESCE(sa.avg_site_time,0) AS avg_site_time 
, COALESCE(sa.med_time_onsite,0) AS med_time_onsite
--CTR details 
, COALESCE(ctr.ctr, imp.med_ctr,0) AS advert_ctr 
, COALESCE(dctr.ctr, dimp.med_ctr,0) AS device_ctr 
, COALESCE(gctr.ctr, glimp.med_ctr,0) AS geo_ctr
, COALESCE(chctr.ctr,chimp.med_ctr,0) AS channel_ctr 
, COALESCE(dowctr.ctr, dowimp.med_ctr,0) AS dayofweek_ctr
, COALESCE(genctr.ctr, genimp.med_ctr,0) AS gender_ctr
, COALESCE(dodctr.dod_ctr_change , 0) AS dod_ctr_change
, COALESCE(wowctr.wow_ctr_change, 0 ) AS wow_ctr_change

-- Number impressions 
, COALESCE(ctr.num_impressions, imp.med_impressions, 0) AS advert_impressions
, COALESCE(dctr.num_impressions, dimp.med_impressions,0) AS device_impressions
, COALESCE(gctr.num_impressions,glimp.med_impressions,0) AS geo_impressions
, COALESCE(chctr.num_impressions,chimp.med_impressions,0) AS channel_impressions
, COALESCE(dowctr.num_impressions,dowimp.med_impressions,0) AS dayofweek_impressions
, COALESCE(genctr.num_impressions,genimp.med_impressions,0) AS gender_impressions
, dodctr.total_impressions AS day_impressions
, dodctr.prior_day_impressions 
, wowctr.total_impressions AS week_impressions 
, wowctr.prior_week_impressions 

-- Views details
, COALESCE(vws.perc_viewtimedow,0) AS perc_viewtimedow
, COALESCE(vws.number_departments_viewed,0) AS number_departments_viewed
, COALESCE(vws.number_pages_viewed,0) number_pages_viewed
, COALESCE(vws.number_pages_viewed_last_week, 0) AS number_pages_viewed_last_week

-- Purchase Details 
, COALESCE(pt.theme_affinity, 0 ) AS purchase_theme_affinity
-- Views/Advert Themes 
, COALESCE(vt.view_theme_score,0) AS view_theme_score
-- Department Affinity
, COALESCE(seg.total_order_value,0) AS perc_order_value_cat_affinity
, COALESCE(seg.prior_30_day_order_value,0) AS perc_30_day_order_value_cat_affinity
, COALESCE(seg.total_order_qty,0) AS perc_order_qty_cat_affinity
, CASE WHEN seg.spend_perc_ranking =1 THEN 1 ELSE 0 END AS highest_spend_cat_alignment

-- Cat_id view
, vaf.highest_associated_catid_weight AS view_highest_catid_weight
, vaf.support12 AS view_support12
, vaf.support1 AS view_support1
, vaf.support2 AS view_support2
, vaf.lift AS view_lift
, vaf.lift_adjusted AS view_lift_adjusted
, vaf.cs AS view_cs
-- Cat_id purchase
, paf.highest_associated_catid_weight AS purchase_highest_catid_weight
, paf.support12 AS purchase_support12
, paf.support1 AS purchase_support1 
, paf.support2 AS purchase_support2
, paf.lift AS purchase_lift
, paf.lift_adjusted AS purchase_lift_adjusted
, paf.cs AS purchase_cs

-- Spending Details
, COALESCE(ts.total_order_value, 0) AS total_order_value
,  COALESCE(ts.prior_30_day_order_value, 0) AS prior_30_day_order_value
, COALESCE(ts.total_number_items,0 ) AS total_number_items
--- Prior Advert impressions/Click
, impr.customer_total_impressions
, impr.customer_total_unique_adverts
, impr.customer_total_clicks
, impr.customer_total_unique_adverts_clicked
, impr.customer_advert_previous_impression_number
, impr.customer_advert_previous_click_number
, impr.number_algodivisions_clicked
, impr.number_algodivisions_impressions
, impr.number_impressions_same_algodivision
, impr.number_clicks_same_algodivision
, impr.number_unique_adverts_same_algodivision
, impr.number_unique_adverts_clicked_same_algodivision
-- Training 
, t.Ad_clicked
, t.treatment_type
, t.location
FROM 
    -- Customer/Ad/Date base
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_sessions_base') AS t
    -- Adverts
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ads_base') AS a
         ON a.UniqueAdID = t.control_sheet_AdID
         ANd a.rundate=t.rundate
    -- Session details 
    LEFT JOIN  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_sessions_aggregation') AS sa
        ON sa.account_number = t.account_number
        AND sa.rundate=t.rundate
    --Overall CTR 
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ctr_summary') AS ctr 
        ON ctr.split_type='Overall'
        -- AND ctr.control_sheet_AdID=a.uniqueAdID
        AND ctr.title=a.title
        AND ctr.campaign=a.campaignnumber
        AND ctr.versionnumber= a.versionnumber
        AND ctr.algodivision=a.algodivision
        AND ctr.rundate=t.rundate
    -- Device CTR 
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ctr_summary') AS dctr 
        ON dctr.split_type='Device'
        AND sa.device_simple = dctr.feature_name
        -- AND dctr.control_sheet_AdID=a.uniqueAdID
        AND dctr.title=a.title
        AND dctr.campaign=a.campaignnumber
        AND dctr.versionnumber= a.versionnumber
        AND dctr.algodivision=a.algodivision
        AND dctr.rundate=t.rundate
    -- GeoLocation CTR 
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ctr_summary') AS gctr 
        ON gctr.split_type='GeoCountry'
        AND sa.geocountry_simple = gctr.feature_name
        -- AND gctr.control_sheet_AdID=a.uniqueAdID
        AND gctr.title=a.title
        AND gctr.campaign=a.campaignnumber
        AND gctr.versionnumber= a.versionnumber
        AND gctr.algodivision=a.algodivision
        AND gctr.rundate=t.rundate
    -- Channel CTR 
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ctr_summary') AS chctr 
        ON chctr.split_type='Channel'
        AND sa.channel_simple = chctr.feature_name
        -- AND chctr.control_sheet_AdID=a.uniqueAdID
        AND chctr.title=a.title
        AND chctr.campaign=a.campaignnumber
        AND chctr.versionnumber= a.versionnumber
        AND chctr.algodivision=a.algodivision
        AND chctr.rundate=t.rundate
    -- DOW CTR 
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ctr_summary') AS dowctr 
        ON dowctr.split_type='DOW'
        AND sa.session_dow = dowctr.feature_name
        -- AND dowctr.control_sheet_AdID=a.uniqueAdID
        AND dowctr.title=a.title
        AND dowctr.campaign=a.campaignnumber
        AND dowctr.versionnumber= a.versionnumber
        AND dowctr.algodivision=a.algodivision
        AND dowctr.rundate=t.rundate

    -- Gender CTR 
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ctr_summary') AS genctr 
        ON genctr.split_type='Gender'
        AND t.gender = genctr.feature_name
        -- AND genctr.control_sheet_AdID=a.uniqueAdID
        AND genctr.title=a.title
        AND genctr.campaign=a.campaignnumber
        AND genctr.versionnumber= a.versionnumber
        AND genctr.algodivision=a.algodivision
        AND genctr.rundate=t.rundate
    -- WOW CTR change
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_wow_ctr') AS wowctr
        -- ON wowctr.control_sheet_AdID=a.uniqueAdID
        ON wowctr.title=a.title
        AND wowctr.campaign=a.campaignnumber
        AND wowctr.versionnumber= a.versionnumber
        AND wowctr.algodivision=a.algodivision
        AND wowctr.week = DATE_TRUNC('week',  t.rundate) - interval '1 week'
    -- DOD CTR change
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_dod_ctr')  AS dodctr
        --   ON dodctr.control_sheet_AdID=a.uniqueAdID
        ON dodctr.title=a.title
        AND dodctr.campaign=a.campaignnumber
        AND dodctr.versionnumber= a.versionnumber
        AND dodctr.algodivision=a.algodivision
        AND dodctr.date = t.rundate- interval '1 day'
    -- Page view info 
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_views_aggregated')  AS vws
        ON vws.account_number = t.account_number
        AND vws.rundate = t.rundate
    -- Page View Themes 
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_view_themes') AS vt 
        ON vt.account_number = t.account_number
        AND vt.themes=a.theme
        AND vt.rundate = t.rundate
    -- Purchase Themes 
     LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_purchase_themes') AS pt 
        ON pt.account_number = t.account_number
        AND pt.themes=a.theme
        AND pt.rundate = t.rundate
    -- Customer AlgoDivsion Segment overlap 
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_customer_segmentation') AS seg
        ON seg.account_number=t.account_number
        AND seg.segment_type='Department'
        AND seg.segment_group=a.AlgoDivision
        AND seg.rundate=t.rundate
    -- Customer Total Purchase Details
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_customer_segmentation') AS ts
        ON ts.account_number=t.account_number
        AND ts.segment_type='Overall'
        AND ts.rundate=t.rundate
    --- CTR Imputations for missing variables
    --Overall CTR 
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ctr_imputation') AS imp 
        ON imp.split_type='Overall'
        AND imp.algodivision=a.algodivision
        AND imp.rundate=t.rundate
    -- Device CTR 
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ctr_imputation') AS dimp 
        ON dimp.split_type='Device'
        AND sa.device_simple = dimp.feature_name
        AND dimp.algodivision=a.algodivision
        AND dimp.rundate=t.rundate
    -- GeoLocation CTR 
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ctr_imputation') AS glimp
        ON glimp.split_type='GeoCountry'
        AND sa.geocountry_simple = glimp.feature_name
        AND glimp.algodivision=a.algodivision
        AND glimp.rundate=t.rundate
    -- Channel CTR 
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ctr_imputation') AS chimp
        ON chimp.split_type='Channel'
        AND sa.channel_simple = chimp.feature_name
        AND chimp.algodivision=a.algodivision
        AND chimp.rundate=t.rundate
    -- DOW CTR 
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ctr_imputation') AS dowimp
        ON dowimp.split_type='DOW'
        AND sa.session_dow = dowimp.feature_name
        AND dowimp.algodivision=a.algodivision
        AND dowimp.rundate=t.rundate
    -- Gender CTR 
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ctr_imputation')  AS genimp
        ON genimp.split_type='Gender'
        AND t.gender = genimp.feature_name
        AND genimp.algodivision=a.AlgoDivision
        AND genimp.rundate=t.rundate
    -- CatID 
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_viewed_latest_advert_catid_affinity') AS vaf
        ON vaf.rundate=t.rundate
        AND vaf.account_number=t.account_number
        AND vaf.control_sheet_adid=t.control_sheet_Adid
     LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_purchased_latest_advert_catid_affinity') AS paf
        ON paf.rundate=t.rundate 
        AND paf.account_number=t.account_number
        AND paf.control_sheet_adid=t.control_sheet_Adid
     LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_customer_advert_impressions') AS impr
        ON t.account_number = impr.account_number
        AND t.rundate = impr.rundate
        AND t.control_sheet_AdID = impr.control_sheet_AdID
);

ALTER TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || :output_table_name)
ALTER COLUMN account_number SET NOT NULL;
ALTER TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || :output_table_name)
ALTER COLUMN uniqueAdID SET NOT NULL;
ALTER TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || :output_table_name)
ALTER COLUMN rundate SET NOT NULL;
ALTER TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || :output_table_name)
ADD PRIMARY KEY (account_number, uniqueAdID, rundate);


-- SELECT rundate, account_number, uniqueAdid, COUNT(*) as count  

-- FROM 
-- IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || :output_table_name) 
-- group by rundate, account_number, uniqueAdid having count > 1 order by count desc
-- ;
