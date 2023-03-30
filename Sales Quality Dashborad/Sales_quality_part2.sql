CREATE TABLE IF NOT EXISTS "lla_cco_int_san"."cwp_sales_quality_dec22_full" as 

WITH 
Parameters AS (
SELECT 
DATE('2022-12-01') AS input_month,
date_trunc('Month' ,current_date) as current_month
)

,part_one as (
select * from "lla_cco_int_san"."cwp_sales_quality_dec22"
)
-- ######################################## Mortlity Rate ###############################################

,forward_months as (
Select date_trunc('MONTH', date(dt)) as month_survival,dt, act_acct_cd, fi_outst_age,case when fi_outst_age is null then '1900-01-01' else cast(date_add('day',-cast(fi_outst_age as int),date(dt)) as varchar) end as oldest_unpaid_bill_dt
from "db-analytics-prod"."fixed_cwp" 
where date(dt) = date_trunc('MONTH',date(dt)) + interval '1' month - interval '1' day and date(dt) between (select input_month from parameters) and (select input_month from parameters) + interval '13' month
and act_acct_cd in (select distinct act_acct_cd from (select * from part_one))
and act_acct_stat != 'C' and pd_mix_cd != '0P'
)


,acct_panel_surv as (
select act_acct_cd ,
max(oldest_unpaid_bill_dt) as max_oldest_unpaid_bill_dt,
max(case when (month_survival = (select input_month from parameters) + interval '0' month  and (fi_outst_age < 90 or fi_outst_age is null)) then 1 
when ( (select input_month from parameters) + interval '0' month  = date ('2022-12-01')) then 1 else null end) as surv_M0,
max(case when month_survival = (select input_month from parameters) + interval '0' month then fi_outst_age else null end) as fi_outst_age_M0,
max(case when (month_survival = (select input_month from parameters) + interval '1' month and (fi_outst_age < 90 or fi_outst_age is null))  then 1 
when ( (select input_month from parameters) + interval '1' month  = date ('2022-12-01')) then 1 else null end) as surv_M1,
max(case when month_survival = (select input_month from parameters) + interval '1' month then fi_outst_age else null end) as fi_outst_age_M1,
max(case when (month_survival = (select input_month from parameters) + interval '2' month and (fi_outst_age < 90 or fi_outst_age is null))  then 1 
when ( (select input_month from parameters) + interval '2' month  = date ('2022-12-01')) then 1 else null end) as surv_M2,
max(case when month_survival = (select input_month from parameters) + interval '2' month then fi_outst_age else null end) as fi_outst_age_M2,
max(case when (month_survival = (select input_month from parameters) + interval '3' month and (fi_outst_age < 90 or fi_outst_age is null))  then 1 
when ( (select input_month from parameters) + interval '3' month  = date ('2022-12-01')) then 1 else null end) as surv_M3,
max(case when month_survival = (select input_month from parameters) + interval '3' month then fi_outst_age else null  end) as fi_outst_age_M3,
max(case when (month_survival = (select input_month from parameters) + interval '4' month and (fi_outst_age < 90 or fi_outst_age is null))  then 1 
when ( (select input_month from parameters) + interval '4' month  = date ('2022-12-01')) then 1 else null end) as surv_M4,
max(case when month_survival = (select input_month from parameters) + interval '4' month then fi_outst_age else null  end) as fi_outst_age_M4,
max(case when (month_survival = (select input_month from parameters) + interval '5' month and (fi_outst_age < 90 or fi_outst_age is null))  then 1 
when ( (select input_month from parameters) + interval '5' month  = date ('2022-12-01')) then 1 else null end) as surv_M5,
max(case when month_survival = (select input_month from parameters) + interval '5' month then fi_outst_age else null  end) as fi_outst_age_M5,
max(case when (month_survival = (select input_month from parameters) + interval '6' month and (fi_outst_age < 90 or fi_outst_age is null))  then 1 
when ( (select input_month from parameters) + interval '6' month  = date ('2022-12-01')) then 1 else null end) as surv_M6,
max(case when month_survival = (select input_month from parameters) + interval '6' month then fi_outst_age else null  end) as fi_outst_age_M6,
max(case when (month_survival = (select input_month from parameters) + interval '7' month and (fi_outst_age < 90 or fi_outst_age is null))  then 1 
when ( (select input_month from parameters) + interval '7' month  = date ('2022-12-01')) then 1 else null end) as surv_M7,
max(case when month_survival = (select input_month from parameters) + interval '7' month then fi_outst_age else null  end) as fi_outst_age_M7,
max(case when (month_survival = (select input_month from parameters) + interval '8' month and (fi_outst_age < 90 or fi_outst_age is null))  then 1 
when ( (select input_month from parameters) + interval '8' month  = date ('2022-12-01')) then 1 else null end) as surv_M8,
max(case when month_survival = (select input_month from parameters) + interval '8' month then fi_outst_age else null  end) as fi_outst_age_M8,
max(case when (month_survival = (select input_month from parameters) + interval '9' month and (fi_outst_age < 90 or fi_outst_age is null)) then 1 
when ( (select input_month from parameters) + interval '9' month  = date ('2022-12-01')) then 1 else null end) as surv_M9,
max(case when month_survival = (select input_month from parameters) + interval '9' month then fi_outst_age else null  end) as fi_outst_age_M9,
max(case when (month_survival = (select input_month from parameters) + interval '10' month and (fi_outst_age < 90 or fi_outst_age is null))  then 1 
when ( (select input_month from parameters) + interval '10' month  = date ('2022-12-01')) then 1 else null end) as surv_M10,
max(case when month_survival = (select input_month from parameters) + interval '10' month then fi_outst_age else null  end) as fi_outst_age_M10,
max(case when (month_survival = (select input_month from parameters) + interval '11' month and (fi_outst_age < 90 or fi_outst_age is null)) then 1 
when ( (select input_month from parameters) + interval '11' month  = date ('2022-12-01')) then 1 else null end) as surv_M11,
max(case when month_survival = (select input_month from parameters) + interval '11' month then fi_outst_age end) as fi_outst_age_M11,
max(case when (month_survival = (select input_month from parameters) + interval '12' month and (fi_outst_age < 90 or fi_outst_age is null))  then 1 
when ( (select input_month from parameters) + interval '12' month  = date ('2022-12-01')) then 1 else null end) as surv_M12,
max(case when month_survival = (select input_month from parameters) + interval '12' month then fi_outst_age end) as fi_outst_age_M12
from forward_months 
group by act_acct_cd
)

,churners as (
select *,
case when surv_m0 is null then 1 else null end as churn_m0,
case when surv_m0 =1 and surv_m1 is null  and (select input_month from parameters) + interval '1' month < (select current_month from parameters) then 1 else null end  as churn_m1,
case when surv_m1 =1 and surv_m2 is null and (select input_month from parameters) + interval '2' month  < (select current_month from parameters) then 1 else null end  as churn_m2,
case when surv_m2 =1 and surv_m3 is null and (select input_month from parameters) + interval '3' month  < (select current_month from parameters) then 1 else null end  as churn_m3,
case when surv_m3 =1 and surv_m4 is null and (select input_month from parameters) + interval '4' month  < (select current_month from parameters) then 1 else null end  as churn_m4,
case when surv_m4 =1 and surv_m5 is null and (select input_month from parameters) + interval '5' month  < (select current_month from parameters) then 1 else null end  as churn_m5,
case when surv_m5 =1 and surv_m6 is null and (select input_month from parameters) + interval '6' month  < (select current_month from parameters) then 1 else null end  as churn_m6,
case when surv_m6 =1 and surv_m7 is null and (select input_month from parameters) + interval '7' month  < (select current_month from parameters) then 1 else null end  as churn_m7,
case when surv_m7 =1 and surv_m8 is null and (select input_month from parameters) + interval '8' month  < (select current_month from parameters) then 1 else null end  as churn_m8, 
case when surv_m8 =1 and surv_m9 is null and (select input_month from parameters) + interval '9' month  < (select current_month from parameters) then 1 else null end  as churn_m9,
case when surv_m9 =1 and surv_m10 is null and (select input_month from parameters) + interval '10' month  < (select current_month from parameters) then 1 else null end  as churn_m10,
case when surv_m10 =1 and surv_m11 is null and (select input_month from parameters) + interval '11' month  < (select current_month from parameters) then 1 else null end  as churn_m11,
case when surv_m11 =1 and surv_m12 is null and (select input_month from parameters) + interval '12' month  < (select current_month from parameters) then 1 else null end  as churn_m12
from acct_panel_surv
)

,churner_type_flag as (
select *,
case when churn_m0 =1 and fi_outst_Age_m0>=90 then 'Involuntary' when churn_m0 =1  and (fi_outst_Age_m0<90 or fi_outst_Age_m0 is null) then 'Voluntary'else null end as churn_type_m0,
case when surv_m1 = 1 and churn_m1 =1 and fi_outst_Age_m1>=90 then 'Involuntary' when surv_m0 = 1 and churn_m1 =1 and (fi_outst_Age_m0<90 or fi_outst_Age_m1 is null)  then  'Voluntary' else null end  as churn_type_m1,
case when surv_m2 = 1 and churn_m2 =1 and fi_outst_Age_m2>=90 then 'Involuntary' when surv_m1 = 1 and churn_m1 =1 and (fi_outst_Age_m0<90 or fi_outst_Age_m2 is null) then  'Voluntary' else null end  as churn_type_m2,
case when surv_m2 = 1 and churn_m3 =1 and fi_outst_Age_m3>=90 then 'Involuntary' when surv_m2 = 1 and churn_m1 =1 and (fi_outst_Age_m0<90 or fi_outst_Age_m3 is null) then  'Voluntary' else null end as churn_type_m3,
case when surv_m3 = 1 and churn_m4 =1 and fi_outst_Age_m4>=90 then 'Involuntary' when surv_m3 = 1 and churn_m1 =1 and (fi_outst_Age_m0<90 or fi_outst_Age_m4 is null) then  'Voluntary' else null end  as churn_type_m4,
case when surv_m4 = 1 and churn_m4 =1 and fi_outst_Age_m5>=90 then 'Involuntary' when surv_m4 = 1 and churn_m1 =1 and (fi_outst_Age_m0<90 or fi_outst_Age_m5 is null) then  'Voluntary' else null end as churn_type_m5,
case when surv_m5 = 1 and churn_m5 =1 and fi_outst_Age_m6>=90 then 'Involuntary' when surv_m5 = 1 and churn_m1 =1 and (fi_outst_Age_m0<90 or fi_outst_Age_m6 is null) then  'Voluntary' else null end as churn_type_m6,
case when surv_m6 = 1 and churn_m6 =1 and fi_outst_Age_m7>=90 then 'Involuntary' when surv_m6 = 1 and churn_m1 =1 and (fi_outst_Age_m0<90 or fi_outst_Age_m7 is null)  then  'Voluntary' else null end as churn_type_m7,
case when surv_m7 = 1 and churn_m7 =1 and fi_outst_Age_m8>=90 then 'Involuntary' when surv_m7 = 1 and churn_m1 =1 and (fi_outst_Age_m0<90 or fi_outst_Age_m8 is null) then  'Voluntary' else null end as churn_type_m8,
case when surv_m8 = 1 and churn_m8 =1 and fi_outst_Age_m9>=90 then 'Involuntary' when surv_m8 = 1 and churn_m1 =1 and (fi_outst_Age_m0<90 or fi_outst_Age_m9 is null) then  'Voluntary' else null end as churn_type_m9,
case when surv_m9 = 1 and churn_m9 =1 and fi_outst_Age_m10>=90 then 'Involuntary' when surv_m9 = 1 and churn_m1 =1 and (fi_outst_Age_m0<90 or fi_outst_Age_m10 is null) then  'Voluntary' else null end as churn_type_m10,
case when surv_m10 = 1 and churn_m10 =1 and fi_outst_Age_m12>=90 then 'Involuntary' when surv_m10 = 1 and churn_m1 =1  and (fi_outst_Age_m0<90 or fi_outst_Age_m11 is null) then  'Voluntary' else null end as churn_type_m11,
case when surv_m11 = 1 and churn_m11 =1 and fi_outst_Age_m12>=90 then 'Involuntary' when surv_m11 = 1 and churn_m1 =1  and (fi_outst_Age_m0<90 or fi_outst_Age_m12 is null) then  'Voluntary' else null end as churn_type_m12
from churners

)

,mortality_view as (
select distinct a.*, 
max_oldest_unpaid_bill_dt,surv_m0,surv_m1,surv_m2,surv_m3,surv_m4,surv_m5,surv_m6,surv_m7,surv_m8,surv_m9,surv_m10,surv_m11,surv_m12,
fi_outst_age_m0,fi_outst_age_m1,fi_outst_age_m2,fi_outst_age_m3,fi_outst_age_m4,fi_outst_age_m5,fi_outst_age_m6,fi_outst_age_m7,fi_outst_age_m8,fi_outst_age_m9,fi_outst_age_m10,fi_outst_age_m11,fi_outst_age_m12,
churn_m0,churn_m1,churn_m2,churn_m3,churn_m4,churn_m5,churn_m6,churn_m7,churn_m8,churn_m9,churn_m10,churn_m11,churn_m12,
churn_type_m0,churn_type_m1,churn_type_m2,churn_type_m3,churn_type_m4,churn_type_m5,churn_type_m6,churn_type_m7,churn_type_m8,churn_type_m9,churn_type_m10,churn_type_m11,churn_type_m12,
case 
when  b.surv_M6 is null and sales_month <> '2022-06-01' then 1 
else 0 end as churners_6_month
from (select * from part_one)  a
inner join churner_type_flag b
on a.act_acct_cd = b.act_acct_cd
)

-- ##################################### Waterfall visualization ################################################

,bill_dt as (
select concat(act_acct_cd,'-',cast(first_bill_dt as varchar)) as act_first_bill,
concat(act_acct_cd,'-',cast(second_bill_dt as varchar)) as act_second_bill,
concat(act_acct_cd,'-',cast(third_bill_dt as varchar)) as act_third_bill
from ( 
    select act_acct_cd, first_bill_dt + interval '1' day as first_bill_dt, second_bill_dt + interval '1' day as second_bill_dt, third_bill_dt + interval '1' day as third_bill_dt
    from (select act_acct_cd,
        cast(TRY(FILTER(ARRAY_AGG(first_bill_amt ORDER BY month_dt), x -> (x IS NOT NULL))[1]) as double) AS first_bill_amt,
        cast(TRY(FILTER(ARRAY_AGG(first_bill_dt ORDER BY month_dt), x -> (x IS NOT NULL))[1]) as date)AS first_bill_dt,
        cast(TRY(FILTER(ARRAY_AGG(first_bill_amt ORDER BY month_dt), x -> (x IS NOT NULL))[2]) as double)AS second_bill_amt,
        cast(TRY(FILTER(ARRAY_AGG(first_bill_dt ORDER BY month_dt), x -> (x IS NOT NULL))[2]) as date)  AS second_bill_dt,
        cast(TRY(FILTER(ARRAY_AGG(first_bill_amt ORDER BY month_dt), x -> (x IS NOT NULL))[3]) as double) AS third_bill_amt,
        cast(TRY(FILTER(ARRAY_AGG(first_bill_dt ORDER BY month_dt), x -> (x IS NOT NULL))[3]) as date) AS third_bill_dt
        from (
            select act_acct_cd , DATE_TRUNC('month', date(dt)) as month_dt,
            TRY(FILTER(ARRAY_AGG(fi_bill_amt_m0 ORDER BY dt), x -> (x IS NOT NULL))[1]) AS first_bill_amt, 
            TRY(FILTER(ARRAY_AGG(fi_bill_dt_m0 ORDER BY dt), x -> (x IS NOT NULL))[1]) AS first_bill_dt,
            TRY(FILTER(ARRAY_AGG(fi_bill_due_dt_m0 ORDER BY dt), x -> (x IS NOT NULL))[1]) AS first_bill_due_dt
            from  "db-analytics-prod"."fixed_cwp"
            WHERE  CAST(act_acct_cd AS BIGINT) in (select cast(act_acct_cd as bigint) from mortality_view)
            group by 1,2
            )
        group by act_acct_cd
        )
    )
)


,first_bill_overdue as(
select act_acct_cd, max(fi_outst_age) as max_fi_outst_age_1st_bill,
min(fi_outst_age) as min_fi_outst_age_1st_bill,
max(date(dt)) as max_dt_1st_bill,
max(oldest_unpaid_bill_dt) as oldest_unpaid_bill_dt_1st_bill
from 
(select act_acct_cd,fi_outst_age,dt,oldest_unpaid_bill_dt
from (select act_acct_cd, fi_outst_age, dt,
 case when fi_outst_age is null then '1900-01-01' else cast(date_add('day',-cast(fi_outst_age as int),date(dt)) as varchar) end as oldest_unpaid_bill_dt
from  "db-analytics-prod"."fixed_cwp"
WHERE  CAST(act_acct_cd AS BIGINT) in (select cast(act_acct_cd as bigint) from mortality_view)
)
where concat(act_acct_cd,'-',oldest_unpaid_bill_dt) in (select act_first_bill from bill_dt)
)
group by act_acct_cd
)

,second_bill_overdue as(
select act_acct_cd, max(fi_outst_age) as max_fi_outst_age_2nd_bill,
min(fi_outst_age) as min_fi_outst_age_2nd_bill, 
max(date(dt)) as max_dt,
max(oldest_unpaid_bill_dt) as oldest_unpaid_bill_dt_2nd_bill
from (select act_acct_cd,fi_outst_age,dt,oldest_unpaid_bill_dt
from (select act_acct_cd, fi_outst_age, dt,
 case when fi_outst_age is null then '1900-01-01' else cast(date_add('day',-cast(fi_outst_age as int),date(dt)) as varchar) end as oldest_unpaid_bill_dt
from  "db-analytics-prod"."fixed_cwp"
WHERE  CAST(act_acct_cd AS BIGINT) in (select cast(act_acct_cd as bigint) from mortality_view)
)
where concat(act_acct_cd,'-',oldest_unpaid_bill_dt) in (select act_second_bill from bill_dt)
)
group by act_acct_cd
)

,third_bill_overdue as(
select act_acct_cd, max(fi_outst_age) as max_fi_outst_age_3rd_bill,
min(fi_outst_age) as min_fi_outst_age_3rd_bill,
max(date(dt)) as max_dt_3rd_bill,
max(oldest_unpaid_bill_dt) as oldest_unpaid_bill_dt_3rd_bill
from 
(select act_acct_cd,fi_outst_age,dt,oldest_unpaid_bill_dt
from (select act_acct_cd, fi_outst_age, dt,
 case when fi_outst_age is null then '1900-01-01' else cast(date_add('day',-cast(fi_outst_age as int),date(dt)) as varchar) end as oldest_unpaid_bill_dt
from  "db-analytics-prod"."fixed_cwp"
WHERE  CAST(act_acct_cd AS BIGINT) in (select cast(act_acct_cd as bigint) from mortality_view)
)
where concat(act_acct_cd,'-',oldest_unpaid_bill_dt) in (select act_third_bill from bill_dt)
)
group by act_acct_cd
)

,bill_cohort_view as (
    SELECT a.*,
    d.max_fi_outst_age_1st_bill,
    d.oldest_unpaid_bill_dt_1st_bill,
    b.max_fi_outst_age_2nd_bill, 
    b.oldest_unpaid_bill_dt_2nd_bill,
    c.max_fi_outst_age_3rd_bill, 
    c.oldest_unpaid_bill_dt_3rd_bill,
    d.min_fi_outst_age_1st_bill,
    b.min_fi_outst_age_2nd_bill,
    c.min_fi_outst_age_3rd_bill
    FROM mortality_view  a
    LEFT JOIN first_bill_overdue d
    on cast(a.act_acct_cd as bigint) = cast(d.act_acct_cd as bigint)
    LEFT JOIN second_bill_overdue b
    on cast(a.act_acct_cd as bigint) = cast(b.act_acct_cd as bigint)
    LEFT JOIN third_bill_overdue c
    on cast(a.act_acct_cd as bigint) = cast(c.act_acct_cd as bigint)
)


,churn_cohort_flags as (
select *, 
--case when max_fi_outst_age_1st_bill >= 60 then 1 else 0 end as fi_60_flag_1st_bill,
--case when max_fi_outst_age_1st_bill >= 90 then 1 else 0 end as fi_90_flag_1st_bill,
--case when max_fi_outst_age_1st_bill >= 60 and churners_6_month = 1 then 1 else 0 end as chuners_60_1st_bill,
case when max_fi_outst_age_1st_bill >= 90 and churners_6_month = 1 then 1 else 0 end as churners_90_1st_bill,
case when DATE_DIFF('day',date(max_oldest_unpaid_bill_dt),date(oldest_unpaid_bill_dt_1st_bill)) < -2 and max_fi_outst_age_1st_bill >= 90 and churners_6_month = 1 then 1 else 0 end as rejoiners_1st_bill,

--case when max_fi_outst_age_2nd_bill >= 60 then 1 else 0 end as fi_60_flag,
--case when max_fi_outst_age_2nd_bill >= 90 then 1 else 0 end as fi_90_flag,
--case when max_fi_outst_age_2nd_bill >= 60 and churners_6_month = 1 then 1 else 0 end as chuners_60_2nd_bill,
case when max_fi_outst_age_2nd_bill >= 90 and churners_6_month = 1 then 1 else 0 end as churners_90_2nd_bill,
case when DATE_DIFF('day',date(max_oldest_unpaid_bill_dt),date(oldest_unpaid_bill_dt_2nd_bill)) < -2 and max_fi_outst_age_2nd_bill >= 90 and churners_6_month = 1 then 1 else 0 end as rejoiners_2nd_bill,

--case when min_fi_outst_age_3rd_bill <=10 and max_fi_outst_age_3rd_bill >= 60 then 1 else 0 end as fi_60_3rd_bill_flag,
--case when min_fi_outst_age_3rd_bill <=10 and max_fi_outst_age_3rd_bill >= 90 then 1 else 0 end as fi_90_3rd_bill_flag,
--case when max_fi_outst_age_3rd_bill >= 60 and churners_6_month = 1 then 1 else 0 end as chuners_60_3rd_bill,
case when max_fi_outst_age_3rd_bill >= 90 and churners_6_month = 1 then 1 else 0 end as churners_90_3rd_bill,
case when DATE_DIFF('day',date(max_oldest_unpaid_bill_dt),date(oldest_unpaid_bill_dt_3rd_bill)) < -2 and max_fi_outst_age_3rd_bill >= 90 and churners_6_month = 1 then 1 else 0 end as rejoiners_3rd_bill

from bill_cohort_view
)
-- select * from churn_cohort_flags where churners_6_month = 1
,vouluntary_churners as (
select *,
case when churners_6_month = 1 and churners_90_1st_bill = 0 and churners_90_2nd_bill = 0 and churners_90_3rd_bill = 0 
then 1 else 0 end as voluntary_churners_6_month
from churn_cohort_flags

)

,waterfall_cohort_view as (
select distinct a.*, churners_90_1st_bill ,churners_90_2nd_bill ,churners_90_3rd_bill, rejoiners_1st_bill,rejoiners_2nd_bill,rejoiners_3rd_bill,voluntary_churners_6_month
from mortality_view a inner join vouluntary_churners b on a.act_acct_cd = b.act_acct_cd
)

-- ############################################### ARPU Calculation #######################################################
,forward_months_mrc as (
Select date_trunc('MONTH', date(dt)) as month_forward,act_acct_cd,max(fi_tot_mrc_amt) as max_mrc, max(rgu_cnt) as rgu_cnt
from ( 
    select dt,act_acct_cd,fi_tot_mrc_amt,
        case 
        when pd_mix_cd = '3P'then 3 
        when pd_mix_cd = '2P' then 2 
        when pd_mix_cd = '1P' then 1 else null end as rgu_cnt
    from "db-analytics-prod"."fixed_cwp" 
    where date(dt) between (select input_month from parameters) and (select input_month from parameters) + interval '14' month
    and date(dt) =  date_trunc('MONTH', date(dt))+ interval '1' month - interval '1' day
    and act_acct_cd in (select distinct act_acct_cd from (select * from part_one))
    and act_acct_stat != 'C' and pd_mix_cd != '0P'
        )
--where date(dt) =  date_trunc('MONTH',date(dt)) + interval '1' month - interval '1' day
group by 1,2
)

,mrc_evo as (
select act_acct_cd,
max(case when month_forward = (select input_month from parameters) + interval '0' month then max_mrc else null end ) as mrc_M0,
max(case when month_forward = (select input_month from parameters) + interval '0' month then rgu_cnt else null end ) as rgu_M0,
max(case when month_forward = (select input_month from parameters) + interval '1' month then max_mrc else null end ) as mrc_M1,
max(case when month_forward = (select input_month from parameters) + interval '1' month then rgu_cnt else null end ) as rgu_M1,
max(case when month_forward = (select input_month from parameters) + interval '2' month then max_mrc else null end ) as mrc_M2,
max(case when month_forward = (select input_month from parameters) + interval '2' month then rgu_cnt else null end ) as rgu_M2,
max(case when month_forward = (select input_month from parameters) + interval '3' month then max_mrc else null end ) as mrc_M3,
max(case when month_forward = (select input_month from parameters) + interval '3' month then rgu_cnt else null end ) as rgu_M3,
max(case when month_forward = (select input_month from parameters) + interval '4' month then max_mrc else null end ) as mrc_M4,
max(case when month_forward = (select input_month from parameters) + interval '4' month then rgu_cnt else null end ) as rgu_M4,
max(case when month_forward = (select input_month from parameters) + interval '5' month then max_mrc else null end ) as mrc_M5,
max(case when month_forward = (select input_month from parameters) + interval '5' month then rgu_cnt else null end ) as rgu_M5,
max(case when month_forward = (select input_month from parameters) + interval '6' month then max_mrc else null end ) as mrc_M6,
max(case when month_forward = (select input_month from parameters) + interval '6' month then rgu_cnt else null end ) as rgu_M6,
max(case when month_forward = (select input_month from parameters) + interval '7' month then max_mrc else null end ) as mrc_M7,
max(case when month_forward = (select input_month from parameters) + interval '7' month then rgu_cnt else null end ) as rgu_M7,
max(case when month_forward = (select input_month from parameters) + interval '8' month then max_mrc else null end ) as mrc_M8,
max(case when month_forward = (select input_month from parameters) + interval '8' month then rgu_cnt else null end ) as rgu_M8,
max(case when month_forward = (select input_month from parameters) + interval '9' month then max_mrc else null end ) as mrc_M9,
max(case when month_forward = (select input_month from parameters) + interval '9' month then rgu_cnt else null end ) as rgu_M9,
max(case when month_forward = (select input_month from parameters) + interval '10' month then max_mrc else null end) as mrc_M10,
max(case when month_forward = (select input_month from parameters) + interval '10' month then rgu_cnt else null end ) as rgu_M10,
max(case when month_forward = (select input_month from parameters) + interval '11' month then max_mrc else null end) as mrc_M11,
max(case when month_forward = (select input_month from parameters) + interval '11' month then rgu_cnt else null end ) as rgu_M11,
max(case when month_forward = (select input_month from parameters) + interval '12' month then max_mrc else null end) as mrc_M12,
max(case when month_forward = (select input_month from parameters) + interval '12' month then rgu_cnt else null end ) as rgu_M12
from forward_months_mrc
group by act_acct_cd
)

,arpu_view as (
select distinct a.*,mrc_m0,mrc_m1,mrc_m2,mrc_m3,mrc_m4,mrc_m5,mrc_m6,mrc_m7,mrc_m8,mrc_m9,mrc_m10,mrc_m11,mrc_m12,
rgu_m0,rgu_m1,rgu_m2,rgu_m3,rgu_m4,rgu_m5,rgu_m6,rgu_m7,rgu_m8,rgu_m9,rgu_m10,rgu_m11,rgu_m12
from waterfall_cohort_view a inner join mrc_evo b on a.act_acct_cd = b.act_acct_cd
)

,deposit_paid as (
select a.*, cuenta as deposit_paid_flag
from arpu_view a left join "db-stage-prod"."cwp_pagos_fijo_agregado"
on act_acct_cd = cast(cuenta as varchar)
)

/*
-- agent view downloadable
,add_info as (
select date_trunc('MONTH', date(a.dt)) as month,a.act_acct_cd,
        a.pd_bb_prod_cd as bb , b.pd_bb_prod_cd as bb_prev,a.pd_tv_prod_cd as tv ,b.pd_tv_prod_cd as tv_prev ,a.pd_vo_prod_cd as vo, b.pd_vo_prod_cd as vo_prev,
        case when a.pd_bb_prod_cd <> b.pd_bb_prod_cd or b.pd_bb_prod_cd is null then a.pd_bb_prod_cd else null end as pd_bb_prod_cd,
        case when a.pd_tv_prod_cd <> b.pd_tv_prod_cd or b.pd_tv_prod_cd is null then a.pd_tv_prod_cd else null end as pd_tv_prod_cd,
        case when a.pd_vo_prod_cd <> b.pd_vo_prod_cd or b.pd_vo_prod_cd is null  then a.pd_vo_prod_cd else null end as pd_vo_prod_cd,
        
        case when a.pd_bb_prod_nm <> b.pd_bb_prod_nm or b.pd_bb_prod_cd is null then a.pd_bb_prod_nm else null end as pd_bb_prod_nm,
        case when a.pd_tv_prod_nm <> b.pd_tv_prod_nm or b.pd_tv_prod_cd is null then a.pd_tv_prod_nm else null end  as pd_tv_prod_nm,
        case when a.pd_vo_prod_nm <> b.pd_vo_prod_nm or b.pd_vo_prod_cd is null then a.pd_vo_prod_nm else null end as pd_vo_prod_nm,
        
        a.cst_cust_cd, a.act_cust_typ,a.act_cust_typ_nm,
        a.act_mktg_cat_cd,
        
        case when a.pd_bb_prod_cd <> b.pd_bb_prod_cd or b.pd_bb_prod_cd is null then a.pd_bb_fmly else null end as pd_bb_fmly,
        case when a.pd_tv_prod_cd <> b.pd_tv_prod_cd or b.pd_tv_prod_cd is null then a.pd_tv_fmly else null end as pd_tv_fmly,
        case when a.pd_vo_prod_cd <> b.pd_vo_prod_cd or b.pd_vo_prod_cd is null then a.pd_vo_fmly else null end as pd_vo_fmly,
        
        case when a.pd_bb_prod_cd <> b.pd_bb_prod_cd or b.pd_bb_prod_cd is null  then a.pd_bb_sbfmly else null end as pd_bb_sbfmly,
        case when a.pd_tv_prod_cd <> b.pd_tv_prod_cd or b.pd_tv_prod_cd is null  then a.pd_tv_sbfmly else null end as pd_tv_sbfmly,
        case when a.pd_vo_prod_cd <> b.pd_vo_prod_cd or b.pd_vo_prod_cd is null  then a.pd_vo_sbfmly else null end as pd_vo_sbfmly,
        
        a.act_rgn_cd,a.act_area_cd,
        a.act_acct_typ_grp,
        a.act_self_install_flg
        from 
        (select * from "db-analytics-prod"."fixed_cwp" where date(dt) between (select input_month from parameters) - interval '3' month and (select input_month from parameters) + interval '1' month) a 
        full outer join 
        (select * from "db-analytics-prod"."fixed_cwp" where date(dt) between (select input_month from parameters) - interval '3' month and (select input_month from parameters) + interval '1' month) b 
        on a.act_acct_cd = b.act_acct_cd and date(a.dt) = date(b.dt) + interval '1' month
        where date(a.dt) = (select input_month from parameters) + interval '1' month - interval '1' day
        and a.act_acct_cd in (select act_acct_cd from deposit_paid)

)

,agent_view as (
    select a.*,bb,tv,vo,bb_prev,tv_prev,vo_prev,pd_bb_prod_cd, pd_tv_prod_cd, pd_vo_prod_cd, pd_bb_prod_nm, pd_tv_prod_nm, pd_vo_prod_nm,cst_cust_cd, act_cust_typ,act_cust_typ_nm,pd_bb_fmly,pd_tv_fmly,pd_vo_fmly,pd_bb_sbfmly,pd_tv_sbfmly,pd_vo_sbfmly,
        act_mktg_cat_cd,act_rgn_cd,act_area_cd,act_acct_typ_grp,act_self_install_flg
        from deposit_paid a inner join add_info b on a.act_acct_cd = b.act_acct_cd 

)
*/
,agent_view as (
 Select date_trunc('MONTH',date(dt)) as month, act_acct_cd, act_acct_name,cst_cust_name, act_cust_typ, act_cust_typ_nm, pd_bb_prod_cd, pd_tv_prod_cd,pd_vo_prod_cd,pd_bb_prod_nm, pd_tv_prod_nm,pd_vo_prod_nm from  "db-analytics-prod"."fixed_cwp" where date(dt) = date_trunc('MONTH',date(dt)) + interval '1' month - interval '1' day
 )
 
,agent_join as (
select a.*, act_acct_name,cst_cust_name, act_cust_typ as customer_type_code, act_cust_typ_nm as customer_type_desc, pd_bb_prod_cd as bb_code, pd_tv_prod_cd as tv_code ,pd_vo_prod_cd as vo_code,pd_bb_prod_nm as bb_name, pd_tv_prod_nm as tv_name,pd_vo_prod_nm as vo_name  from deposit_paid a left join agent_view b on a.act_acct_cd = b.act_acct_cd and cast(month as varchar) = sales_month
)

--select * from deposit_paid limit 10000
 ,duplicates_fix as (
 select distinct *,row_number() over (partition by sales_month,act_acct_cd,deposit_paid_flag order by techflag) as duplicates from agent_join   
 )

 select * from duplicates_fix where duplicates = 1
