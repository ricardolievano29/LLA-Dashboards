with
Accounts_mort_no_npn as (
SELECT sales_channel, movement_flag, count(distinct act_acct_cd)
-- ,fi_outst_age_m5
FROM "lla_cco_int_san"."cwp_sales_quality" where sales_month = '2022-09-01' and surv_m5 is null  and "npn_90_flag" is null and sales_channel = 'D2D'  group by 1,2
)

-- select dt,act_acct_cd,fi_outst_age,fi_tot_mrc_amt from "db-analytics-prod"."fixed_cwp" where date(dt) between date('2023-01-28') and date('2023-02-28') and act_acct_cd in (select act_acct_cd from accounts_mort_no_npn ) order by 2,1


SELECT distinct account_id,fi_tot_mrc_amt, sum(cast(payment_amt_usd as double)) FROM "db-stage-prod"."payments_cwp" a left join (select act_acct_cd,max(fi_tot_mrc_amt) as fi_tot_mrc_amt from "db-analytics-prod"."fixed_cwp" where  date(dt) between date('2022-09-01') and date('2022-11-30')group by 1) on act_acct_cd = account_id where account_id in (select act_acct_cd from accounts_mort_no_npn) and date(a.dt) between date('2022-09-01') and date('2022-12-31') group by 1,2

-- Accounts_mort_no_npn as (
-- SELECT sales_channel,  count(distinct act_acct_cd)
-- -- ,surv_m3,surv_m4,surv_m5 
-- FROM "lla_cco_int_san"."cwp_sales_quality" where sales_month = '2022-09-01' and surv_m5 is null  and "npn_90_flag" is null and sales_channel = 'D2D' group by 1 
-- )

SELECT distinct account_id, sum(cast(payment_amt_usd as double)) FROM "db-stage-prod"."payments_cwp" where account_id in ('321086360000') and date(dt) between date('2022-09-01') and date('2022-11-30') group by 1
