create or replace temporary view 0_theme_mapping as
SELECT  
  distinct *, regexp_replace(theme, '[^a-zA-Z0-9]', '') as theme_clean
FROM
  warehouse.next_uk_nextads_item_themes_latest
  -- where rundate = "2026-02-04"
  where theme_rank = 1