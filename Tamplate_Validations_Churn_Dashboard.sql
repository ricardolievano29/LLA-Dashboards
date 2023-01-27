select *  from "lla_cco_int_san"."cr_fmc_table_onlyfixed"  limit 1

-- Customer View Total churners
select month, b_fmc_type, final_churn_flag, partial_total_churn_flag, count(final_account) as users   from "lla_cco_int_san"."cr_fmc_table_onlyfixed" where month >= date('2022-11-01') and final_churn_flag = 'Full Churner'group by 1,2,3,4

-- Customer full churners - type (Vol - Invol)
select month, churn_type_final_flag, count(final_account) as users   from "lla_cco_int_san"."cr_fmc_table_onlyfixed" where month >= date('2022-11-01') and final_churn_flag = 'Full Churner' group by 1,2

-- Customer full churner - Tenure
select month, b_tenure_final_flag, count(final_account) as users   from "lla_cco_int_san"."cr_fmc_table_onlyfixed" where month >= date('2022-11-01') and final_churn_flag = 'Full Churner' group by 1,2

-- Customer full churner - tech
select month, b_final_tech_flag, count(final_account) as users   from "lla_cco_int_san"."cr_fmc_table_onlyfixed" where month >= date('2022-11-01') and final_churn_flag = 'Full Churner' group by 1,2

-- Customer partial ( only fixed )
select month,b_fmc_type, count(final_account) as users   from "lla_cco_int_san"."cr_fmc_table_onlyfixed" where month >= date('2022-11-01') and main_movement = '03. Downsell' group by 1,2

 -- Cusotmer partial - type ( only fixed )
select month,churn_type_final_flag, count(final_account) as users   from "lla_cco_int_san"."cr_fmc_table_onlyfixed" where month >= date('2022-11-01') and main_movement = '03. Downsell' group by 1,2

 -- Cusotmer  partial - tech ( only fixed )
select month, b_final_tech_flag, count(final_account) as users   from "lla_cco_int_san"."cr_fmc_table_onlyfixed" where month >= date('2022-11-01') and main_movement = '03. Downsell' group by 1,2

 -- Cusotmer partial - type 
select month,churn_type_final_flag, count(final_account) as users   from "lla_cco_int_san"."cr_fmc_table_onlyfixed" where month >= date('2022-11-01') and partial_total_churn_flag = 'Partial Churner' group by 1,2
 -- Cusotmer  partial - tech
select month, b_final_tech_flag, count(final_account) as users   from "lla_cco_int_san"."cr_fmc_table_onlyfixed" where month >= date('2022-11-01') and partial_total_churn_flag = 'Partial Churner' group by 1,2

-- MRC changes 
select month, b_fmc_type,  count(final_account) as users   from "lla_cco_int_san"."cr_fmc_table_onlyfixed" where month >= date('2022-11-01') and mrc_change < 0 and b_total_rgus=e_total_rgus group by 1,2

-- MRC changes - tech
select month, b_final_tech_flag,  count(final_account) as users   from "lla_cco_int_san"."cr_fmc_table_onlyfixed" where month >= date('2022-11-01') and mrc_change < 0 and b_total_rgus=e_total_rgus group by 1,2

-- RGU View
select month, waterfall_flag,churn_type_final_flag,  sum(b_total_rgus) - sum(case when e_total_rgus is null then 0 else e_total_rgus end)
from "lla_cco_int_san"."cr_fmc_table_onlyfixed" where month >= date('2022-11-01')and waterfall_flag in ('Downsell','Full Churner')  group by 1,2,3

-- RGUs - Tenure
select month, b_tenure_final_flag,  sum(b_total_rgus) - sum(case when e_total_rgus is null then 0 else e_total_rgus end)
from "lla_cco_int_san"."cr_fmc_table_onlyfixed" where month >= date('2022-11-01')and waterfall_flag in ('Downsell','Full Churner')  group by 1,2

-- RGUs - Tech
select month, b_final_tech_flag,  sum(b_total_rgus) - sum(case when e_total_rgus is null then 0 else e_total_rgus end)
from "lla_cco_int_san"."cr_fmc_table_onlyfixed" where month >= date('2022-11-01')and waterfall_flag in ('Downsell','Full Churner')  group by 1,2

-- Customer - Tech - Type
select month, b_final_tech_flag,churn_type_final_flag , count(distinct final_account) from "lla_cco_int_san"."cr_fmc_table_onlyfixed" where month >= date('2022-11-01')and waterfall_flag in ('Downsell','Full Churner')  group by 1,2,3

-- Customer Tech - Tenure - Type
select month, b_final_tech_flag,churn_type_final_flag ,b_tenure_final_flag, count(distinct final_account) from "lla_cco_int_san"."cr_fmc_table_onlyfixed" where month >= date('2022-11-01')and waterfall_flag in ('Full Churner')  group by 1,2,3,4 order by 1,2,3,4






