
SELECT sales_month, movement_flag,
-- customers
count(distinct act_acct_cd) as Sales,
sum(churners_90_1st_bill) as Churners_1st_bill,
sum("rejoiners_1st_bill") as Rejoiners_1st_bill, 
sum("churners_90_2nd_bill") as Churners_2nd_bill,
sum("rejoiners_2nd_bill") as Rejoiners_2nd_bill, 
sum("churners_90_3rd_bill") as Churners_3rd_bill,
sum("rejoiners_3rd_bill") as Rejoiners_3rd_bill, 
sum("voluntary_churners_6_month") as Voluntary_churners, 

--rgus
sum(rgus_sold) as Sales_rgu, 
sum(case when churners_90_1st_bill = 1 then rgus_sold else null end ) as Churners_1st_bill_rgu,
sum(case when churners_90_2nd_bill = 1 then rgus_sold else null end) as Churners_2nd_bill_rgu,
sum(case when churners_90_3rd_bill = 1 then rgus_sold else null end) as Churners_3rd_bill_rgu,
sum(case when rejoiners_1st_bill = 1 then rgus_sold  else null end) as rejoiners_1st_bill_rgu,
sum(case when rejoiners_2nd_bill = 1 then rgus_sold else null end)  as rejoiners_2nd_bill_rgu,
sum(case when rejoiners_3rd_bill = 1 then rgus_sold else null end) as rejoiners_3rd_bill_rgu,
sum(case when voluntary_churners_6_month = 1 then rgus_sold else null end)as rejoiners_3rd_bill_rgu
from "lla_cco_int_san"."cwp_sales_quality" group by 1,2
