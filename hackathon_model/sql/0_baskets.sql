SELECT DISTINCT
  account_number,
  itemno,
  order_date,
  sum(s740orderstakenvalue) as s740orderstakenvalue, current_date() as rundate
FROM warehouse.baskets_uk_3y
WHERE order_date between date"{start_date}" and date"{end_date}"
  AND s740orderstakenqty > 0
  AND s740returnsqty = 0
  and clientid like 'N%'
  --TODO: do we want FP only?
GROUP BY 1,2,3
