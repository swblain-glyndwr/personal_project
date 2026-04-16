WITH 
  -- 1. Gather all events from Layer 0 (Views & ATBs) to get Platform & Timestamps
  all_events AS (
    SELECT 
      AccountNumber_RPID as account_number,
      CAST(date AS DATE) as event_date,
      timestamp,
      type as platform, -- 'web' or 'app'
      'view' as event_type
    FROM {catalog}.{table_prefix}_views
    WHERE date between date"{start_date}" and date"{end_date}"
    
    UNION ALL
    
    SELECT 
      AccountNumber_RPID as account_number, 
      CAST(date AS DATE) as event_date,
      timestamp,
      type as platform,
      'atb' as event_type
    FROM {catalog}.{table_prefix}_atbs
    WHERE date between date"{start_date}" and date"{end_date}"
  ),

  -- 2. Aggregate Theme Activity (using Layer 1 for cleaner mapped data)
  theme_activity AS (
    SELECT
      account_number,
      theme_clean
    FROM {catalog}.{table_prefix}_views_themes
    WHERE reference_date = date"{reference_date}"
    UNION ALL
    SELECT
      account_number,
      theme_clean
    FROM {catalog}.{table_prefix}_atbs_themes
    WHERE reference_date = date"{reference_date}"
  ),

  -- 3. Calculate User-Level Aggregates
  user_stats AS (
    SELECT
      account_number,
      
      -- Activity Volume
      count(*) as total_interactions,
      count_if(event_type = 'atb') as total_atbs,
      count_if(event_type = 'view') as total_views,
      
      -- Platform Affinity
      count_if(platform = 'app') / count(*) as app_affinity_ratio, -- 1.0 = All App, 0.0 = All Web
      
      -- Velocity: Activity in last 7 days vs last 28 days (normalized by days)
      -- (7d count / 7) / (28d count / 28)
      -- A value > 1.0 means acceleration (doing more now than average)
      (count_if(event_date >= date"{reference_date}" - 7) / 7.0) / 
        NULLIF(count_if(event_date >= date"{reference_date}" - 28) / 28.0, 0) as velocity_7d_28d,
        
      -- Timing preferences
      approx_percentile(hour(timestamp), 0.5) as median_hour_of_day,
      count_if(dayofweek(event_date) IN (1, 7)) / count(*) as weekend_shopper_ratio
      
    FROM all_events
    GROUP BY 1
  ),
  
  -- 4. Calculate Diversity (Breadth)
  breadth_stats AS (
    SELECT
      account_number,
      count(distinct theme_clean) as distinct_themes_interacted
    FROM theme_activity
    GROUP BY 1
  )

SELECT
  date"{reference_date}" as reference_date,
  u.account_number,
  
  -- Volume Features
  coalesce(u.total_interactions, 0) as user_total_interactions,
  coalesce(u.total_views, 0) as user_total_views,
  
  -- Conversion Features
  -- Guard against division by zero
  coalesce(u.total_atbs / nullif(u.total_views, 0), 0) as user_view_to_atb_rate,
  
  -- Platform Features
  coalesce(u.app_affinity_ratio, 0) as user_app_ratio,
  CASE 
    WHEN u.app_affinity_ratio >= 0.8 THEN 'App_Dominant'
    WHEN u.app_affinity_ratio <= 0.2 THEN 'Web_Dominant'
    ELSE 'Mixed_Platform'
  END as user_platform_segment,
  
  -- Velocity Features
  coalesce(u.velocity_7d_28d, 0) as user_velocity_score,
  
  -- Diversity Features
  coalesce(b.distinct_themes_interacted, 0) as user_theme_breadth,
  
  -- Timing Features
  coalesce(u.weekend_shopper_ratio, 0) as user_weekend_ratio,
  coalesce(u.median_hour_of_day, 12) as user_median_hour, 
  current_date() as rundate

FROM user_stats u
LEFT JOIN breadth_stats b USING (account_number)
