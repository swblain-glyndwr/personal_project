CREATE OR REPLACE TEMPORARY VIEW 0_base as 
SELECT distinct account_number
FROM warehouse.baskets_uk_3y
where ordertakendate between date"{start_date}" and date"{end_date}"


-- union distinct accountnums that have viewed within the last x dates