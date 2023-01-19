WITH
U_0P as (
select distinct  fixed_month,count(distinct fixed_account), sum(e_num_rgus)
FROM "lla_cco_int_san"."cr_fixed_table_ENE16"
where  fixed_month <= date('2022-12-01') and fixed_rejoiner is not null and fixed_rejoiner_type ='2. Fixed Involuntary Rejoiner' -- and fixed_churner_type is not null
group by 1
)

,datos as(
select date(dt) as dy,fixed_month, act_acct_cd,
CASE WHEN pd_vo_prod_nm IS NOT NULL and pd_vo_prod_nm <>'' THEN 1 ELSE 0 END AS RGU_VO,
CASE WHEN pd_tv_prod_nm IS NOT NULL and pd_tv_prod_nm <>'' THEN 1 ELSE 0 END AS RGU_TV,
CASE WHEN pd_bb_prod_nm IS NOT NULL and pd_bb_prod_nm <>'' THEN 1 ELSE 0 END AS RGU_BB
FROM "db-analytics-dev"."dna_fixed_cr" inner join U_0P on act_acct_cd = fixed_account and date_trunc('MONTH', date(dt)) = fixed_month and date(dt) >= date('2022-12-02')
order by 3,1
)
select count(distinct act_acct_cd) from datos where rgu_vo+rgu_tv+rgu_bb > 0
