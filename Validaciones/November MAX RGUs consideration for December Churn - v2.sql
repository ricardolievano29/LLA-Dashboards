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
select count(distinct fixed_account) from Voluntary_churn_diciembre where b_num_rgus>rgu_vo+rgu_tv+rgu_bb and b_mix = '1P' 


,dx_order_nov as (
select distinct  account_name from "db-stage-dev"."so_cr_deprecated"  where date_trunc('MONTH',date(completed_date)) = date('2022-11-01') and order_status = 'FINALIZADA' and order_type = 'DESINSTALACION'
)
,dx_order_dic as (
select distinct  account_name  from "db-stage-dev"."so_cr_deprecated"  where date_trunc('MONTH',date(completed_date)) = date('2022-12-01') and order_status = 'FINALIZADA' and order_type = 'DESINSTALACION'
union all
select distinct account_name  from "db-stage-dev"."so_cr" where date_trunc('MONTH',date(completed_date)) = date('2022-12-01') and order_status = 'FINALIZADA' and order_type = 'DESINSTALACION'
)

,so as (
select distinct account_name from dx_order_nov union all select * from dx_order_dic
)

--select count( distinct fixed_account) from Voluntary_churn_diciembre --a left join dx_order_dic b on fixed_account = account_name where b.account_name is null
,so_vol as (
select distinct fixed_month,fixed_account as users,b_mix, b_num_rgus, rgu_tv+rgu_vo+rgu_bb as new_rgus, b.account_name as so_flag 
from Voluntary_churn_diciembre a left join so b on fixed_account = account_name 
)

,identificacion_downsell as (
select b.*,a.main_movement, a.fixed_churner_type from  so_vol b left join "lla_cco_int_san"."cr_fixed_table_ENE16" a on a.fixed_account = users and a.fixed_month = date_add('MONTH', -1,b.fixed_month) 
)

select count(distinct users) from identificacion_downsell where main_movement = '03. Downsell'
