----- Sample caracterización churners noviembre -------

/*
SELECT fixed_month,main_movement,fixed_churner_type, count(fixed_account),sum(b_num_rgus),sum(e_num_rgus) 
FROM "lla_cco_int_san"."cr_fixed_table_ENE12" 
where fixed_month >= date('2022-11-01') and (fixed_churner_type in ('1. Fixed Voluntary Churner', '2. Fixed Involuntary Churner') or main_movement = '03. Downsell')
group by 1,2,3
*/

----- churners noviembre --------


WITH
Churn_nov as(
select *
/*fixed_month
,fixed_account
,main_movement
,fixed_churner_type
,b_num_rgus
*/
--,case when e_num_rgus is null then 0 else e_num_rgus end as e_num_rgus 
,case when e_num_rgus is null then b_num_rgus - 0 else b_num_rgus - e_num_rgus end as churned_rgus 
FROM "lla_cco_int_san"."cr_fixed_table_ENE12"
WHERE fixed_month = date('2022-11-01') and  (fixed_churner_type in ('1. Fixed Voluntary Churner', '2. Fixed Involuntary Churner') or main_movement = '03. Downsell')
)

,dna_info as (
SELECT distinct date_trunc('MONTH', date(dt)) as month,act_acct_cd, org_cntry--,act_acct_cd,act_cust_type,act_acct_stat,act_cust_start_dt,act_acct_inst_dt
,min(fi_outst_age) as min_fi_outst_age,max(fi_outst_age) as max_fi_outst_age --, pd_vo_prod_nm,pd_tv_prod_nm,pd_bb_prod_nm,
FROM "db-analytics-dev"."dna_fixed_cr" 
WHERE date_trunc('MONTH',date(dt))= date('2022-11-01')
group by 1,2,3
)
,final as(
SELECT distinct  b.*,a.org_cntry,min_fi_outst_age,max_fi_outst_age
FROM dna_info a inner join churn_nov b on act_acct_cd = fixed_account and b.fixed_month = a.month
)

select distinct *--fixed_month, count(fixed_account), sum(churned_rgus) 
from final 
