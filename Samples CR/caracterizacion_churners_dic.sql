----- Sample carcterizacion churners diciembre ---------

----- churners noviembre --------
WITH
Users_churn_dic as(
select distinct fixed_account--,e_num_rgus,(b_num_rgus)
from "lla_cco_int_san"."cr_fixed_table_ENE12" 
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
from users_churn_dic left join rgus_nov on act_acct_cd = fixed_account
where date(dt) between date('2022-11-01') and date('2022-12-31')
group by 1--,2,3,4
)

,Churn_dic as(
select a.*
/*fixed_month
,fixed_account
,main_movement
,fixed_churner_type
,b_num_rgus
*/
--,case when e_num_rgus is null then 0 else e_num_rgus end as e_num_rgus 
,case when e_num_rgus is null then rgu_vo+rgu_tv+rgu_bb - 0 else rgu_vo+rgu_tv+rgu_bb - e_num_rgus end as churned_rgus 
FROM "lla_cco_int_san"."cr_fixed_table_ENE12" a inner join max_rgus on fixed_account = act_acct_cd
WHERE fixed_month = date('2022-12-01') and (fixed_churner_type in ('1. Fixed Voluntary Churner', '2. Fixed Involuntary Churner') or main_movement = '03. Downsell')
)

,dna_info as (
SELECT distinct date_trunc('MONTH', date(dt)) as month,act_acct_cd, org_cntry--,act_acct_cd,act_cust_type,act_acct_stat,act_cust_start_dt,act_acct_inst_dt
,min(fi_outst_age) as min_fi_outst_age,max(fi_outst_age) as max_fi_outst_age --, pd_vo_prod_nm,pd_tv_prod_nm,pd_bb_prod_nm,
FROM "db-analytics-dev"."dna_fixed_cr" 
WHERE date_trunc('MONTH',date(dt))= date('2022-12-01')
group by 1,2,3
)
,final as(
SELECT distinct  b.*,a.org_cntry,min_fi_outst_age,max_fi_outst_age
FROM dna_info a inner join churn_dic b on act_acct_cd = fixed_account and b.fixed_month = a.month
)

select distinct *--fixed_month, count(fixed_account), sum(churned_rgus) 
from final 

