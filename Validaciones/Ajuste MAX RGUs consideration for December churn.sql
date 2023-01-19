-- ========================================================================================
----- Ajuste de RGUs: considerar el máximo de RGUs del mes pasado ----------------------
-- ========================================================================================
WITH
Usuarios_churn_dic as(
select distinct fixed_account--,e_num_rgus,(b_num_rgus)
from "lla_cco_int_san"."cr_fixed_table_ENE16" 
where (fixed_churner_type in ( '1. Fixed Voluntary Churner','2. Fixed Involuntary Churner') or main_movement = '03. Downsell') 
and
fixed_month = date('2022-12-01')
)

,rgus_nov as(
select distinct dt,act_acct_cd, 
case when (pd_vo_prod_nm) is not null then 1 else 0 end as rgu_vo,
case when (pd_tv_prod_nm) is not null then 1 else 0 end as rgu_tv,
case when (pd_bb_prod_nm) is not null then 1 else 0 end as rgu_bb
from "db-analytics-dev"."dna_fixed_cr" 
where date(dt) > date('2022-11-01') -- between date('2022-11-27') and date('2022-11-30')
--group by 1
)
,max_rgus as(
select distinct-- fixed_month,main_movement,b_mix,fixed_churner_type,
act_acct_cd, 
max(rgu_vo) as rgu_vo,
max(rgu_tv) as rgu_tv,
max(rgu_bb)  as rgu_bb
from usuarios_churn_dic left join rgus_nov on act_acct_cd = fixed_account
where date(dt) between date('2022-11-01') and date('2022-12-31')
group by 1--,2,3,4
)

,final as (
select distinct * from "lla_cco_int_san"."cr_fixed_table_ENE16" left join max_rgus on act_acct_cd = fixed_account
)

/* Check de cantidad ususarios y rgus antes y despues del ajuste
select distinct fixed_month, main_movement, fixed_churner_type, count(distinct fixed_account) as users, sum(b_num_rgus) as rgus_before, sum(rgu_vo+rgu_tv+rgu_bb) as new_rgus, sum(e_num_rgus) as final_rgus
from final
group by 1,2,3
*/

,Voluntary_churn_diciembre as (
select distinct * from final where fixed_churner_type = '1. Fixed Voluntary Churner' and fixed_month  = date('2022-12-01')
)
--select count(distinct fixed_account) as users, sum(b_num_rgus) as rgus_before, sum(rgu_vo+rgu_tv+rgu_bb) as rgus_new from Voluntary_churn_diciembre

/* Chek numeros de aument o en el rgu churn con el ajuste de considerar el maximo de rgus en los meses de noviembre y dicimebre
select count(fixed_account)
, sum(b_num_rgus) as ini_rgus
, sum(rgu_vo+rgu_tv+rgu_bb) as new_rgus
, sum(case when e_num_rgus is null then 0 else e_num_rgus end) as end_rgus
, sum(b_num_rgus - case when e_num_rgus is null then 0 else e_num_rgus end) as ini_rgu_churn
, sum(rgu_vo+rgu_tv+rgu_bb- case when e_num_rgus is null then 0 else e_num_rgus end) as end_rgu_churn
from Voluntary_churn_diciembre
where rgu_vo+rgu_tv+rgu_bb > b_num_rgus
*/


-- usuarios a los que los rgus cambiaron con el ajuste de rgus_máximos en los meses de nov y dec
,usuarios_add as (
select *
--fixed_account,rgu_vo,rgu_tv,rgu_bb,b_num_rgus,b_mix
from Voluntary_churn_diciembre where rgu_vo+rgu_tv+rgu_bb > b_num_rgus
)


-- Validacion que los ususarios 0P no recuperan los RGUs en diciembre
,non_P as(
select distinct  fixed_account from usuarios_add where b_mix = '0P'
)

--select count(distinct fixed_account)--, pd_tv_prod_nm, pd_bb_prod_nm, pd_vo_prod_nm 
--from non_P inner join "db-analytics-dev"."dna_fixed_cr" on act_acct_cd = fixed_account where date_trunc('MONTH', date(dt)) = date('2022-12-01') and (pd_tv_prod_nm is not null or pd_bb_prod_nm is not null or pd_vo_prod_nm is not null) 

-- Validacion que los ususarios 1P con mas RGUs al considerar los maximos del mes de noviembre son downsells
select distinct b.main_movement, count(distinct a.fixed_account) --, sum(a.b_num_rgus) as rgus_before, sum(a.rgu_vo+a.rgu_tv+a.rgu_bb) as rgus_new,

from 
usuarios_add a inner join final b on a.fixed_account = b.fixed_account and a.fixed_month = date_add('month',1,b.fixed_month)  and a.b_mix = '1P'
group by 1

,validation_downsells as(
select distinct b.main_movement,a.b_mix,a.fixed_account as users, (a.b_num_rgus) as rgus_before, (rgu_vo+rgu_tv+rgu_bb) as rgus_new, (b.e_num_rgus) as rgus_end_period
from usuarios_add a inner join "lla_cco_int_san"."cr_fixed_table_ENE16" b on a.fixed_account = b.fixed_account 
where b.fixed_month = date('2022-11-01') and b.main_movement = '03. Downsell'
)



--  ordenes de desconexión en noviembre 
,dx_order_historic as (
select distinct  account_name from "db-stage-dev"."so_cr_deprecated"  where date_trunc('MONTH',date(completed_date)) = date('2022-11-01') and order_status = 'FINALIZADA' and order_type = 'DESINSTALACION'
)

select  distinct users as users, rgus_before as rgus_before, rgus_new as rgus_new from validation_downsells left join dx_order_historic on users = account_name where account_name is null

-- Orden de desconexión dicimebre
,dx_order_dic as (
select distinct  account_name  from "db-stage-dev"."so_cr_deprecated"  where date_trunc('MONTH',date(completed_date)) = date('2022-12-01') and order_status = 'FINALIZADA' and order_type = 'DESINSTALACION'
union all
select distinct account_name  from "db-stage-dev"."so_cr" where date_trunc('MONTH',date(completed_date)) = date('2022-12-01') and order_status = 'FINALIZADA' and order_type = 'DESINSTALACION'
)

,so as(
select * from dx_order_historic
union all
select * from dx_order_dic

)

--select count(distinct users), sum(rgus_before),sum(rgus_new) from validation_downsells inner join so on users = account_name 


/*
,dx_order_dic_p2 as(
select distinct * from "db-stage-dev"."so_cr" where date_trunc('MONTH',date(completed_date)) = date('2022-12-01') and order_status = 'FINALIZADA' and order_type = 'DESINSTALACION'
)

,dx_order_dic as(
select distinct account_name from dx_order_dic_p1 union all select account_name from dx_order_dic_p2
)
*/
/*
,user_add as (
select distinct (a.fixed_account),(a.b_num_rgus) as rgus_antes,(rgu_vo + rgu_tv + rgu_bb) as rgus_fix,(rgu_vo + rgu_tv + rgu_bb - a.b_num_rgus) as diff
--a.fixed_month,b.fixed_month, a.fixed_account,a.main_movement,b.main_movement,a.fixed_churner_type,b.fixed_churner_type 
from final a inner join "lla_cco_int_san"."cr_fixed_table_ENE12" b on b.fixed_account = a.fixed_account inner join Voluntary_churn_diciembre on b.fixed_account = c.fixed_account 
and date('2022-12-01') = a.fixed_month and b.fixed_month = date_add('MONTH',-1,a.fixed_month) 
where (rgu_vo + rgu_tv + rgu_bb) > a.b_num_rgus and a.fixed_churner_type = '1. Fixed Voluntary Churner' --and b.main_movement <>'03. Downsell'
)
*/

