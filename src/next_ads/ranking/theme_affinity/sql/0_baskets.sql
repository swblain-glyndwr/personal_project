SELECT DISTINCT
  account_number,
  itemno,
  order_date,
  sum(s740orderstakenvalue) as s740orderstakenvalue, current_date() as rundate
FROM marketingdata_prod.warehouse.baskets_uk_3y
WHERE order_date between date"{start_date_baskets}" and date"{target_month_end}"
  AND s740orderstakenqty > 0
  AND s740returnsqty = 0
  and clientid like 'N%'
  --TODO: do we want FP only?
GROUP BY 1,2,3
