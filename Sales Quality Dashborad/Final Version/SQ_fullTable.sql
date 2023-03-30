CREATE TABLE IF NOT EXISTS "lla_cco_int_san"."cwp_sales_quality"  AS

select * from "lla_cco_int_san"."cwp_sales_quality_ene22_full"
union all
select * from "lla_cco_int_san"."cwp_sales_quality_feb22_full"
union all
select * from "lla_cco_int_san"."cwp_sales_quality_mar22_full"
union all
select * from "lla_cco_int_san"."cwp_sales_quality_apr22_full"
union all
select * from "lla_cco_int_san"."cwp_sales_quality_may22_full"
union all
select * from "lla_cco_int_san"."cwp_sales_quality_jun22_full"
union all
select * from "lla_cco_int_san"."cwp_sales_quality_jul22_full"
union all
select * from "lla_cco_int_san"."cwp_sales_quality_ago22_full"
union all
select * from "lla_cco_int_san"."cwp_sales_quality_sep22_full"
union all
select * from "lla_cco_int_san"."cwp_sales_quality_oct22_full"
union all
select * from "lla_cco_int_san"."cwp_sales_quality_nov22_full"
union all
select * from "lla_cco_int_san"."cwp_sales_quality_dec22_full"
union all
select * from "lla_cco_int_san"."cwp_sales_quality_ene23_full"
