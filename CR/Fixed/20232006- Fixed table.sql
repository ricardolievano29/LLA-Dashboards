-----------------------------------------------------------------------
                    -- VERSION FINAL FIXED TABLE--
-----------------------------------------------------------------------
-- CREATE TABLE IF NOT EXISTS "lla_cco_int_san"."cr_fixed_table_jun18_apr_v2"  AS  
WITH 

Parameters as(
Select 
    date('2023-04-01') as input_month,
   92 as InvoluntaryChurnDays
)

,UsefulFields AS(
SELECT DISTINCT 
    DATE_TRUNC ('Month' , cast(a.dt as date)) AS month, 
    a.dt,
    act_acct_cd, cst_cust_cd,
    pd_vo_prod_nm, PD_TV_PROD_nm, pd_bb_prod_nm,
    fi_outst_age, Customer_age,fi_overdue_age, 
    first_value (ACT_ACCT_INST_DT) over(PARTITION  BY act_acct_cd ORDER BY a.dt ASC) AS MinInst,
    first_value (ACT_ACCT_INST_DT) over(PARTITION  BY act_acct_cd ORDER BY ACT_ACCT_INST_DT DESC) AS MaxInst,
    DATE_DIFF('DAY',cast(OLDEST_UNPAID_BILL_DT as date), cast(a.dt as date)) AS MORA,
    ACT_CONTACT_MAIL_1,act_contact_phone_1,
    round(FI_VO_MRC_AMT,0) AS mrcVO, round(FI_BB_MRC_AMT,0) AS mrcBB, round(FI_TV_MRC_AMT,0) AS mrcTV,
    
    round((FI_VO_MRC_AMT + FI_BB_MRC_AMT + FI_TV_MRC_AMT),0)  as avgmrc, 
    --round(FI_BILL_AMT_M0,0) AS Bill,
    ACT_CUST_STRT_DT,
    --lst_pymt_dt,
    oldest_unpaid_bill_dt,
    
    CASE WHEN cardinality(pd_vo_prod_nm) <> 0  
    THEN 1 ELSE 0 END AS RGU_VO,
    CASE WHEN cardinality(pd_tv_prod_nm) <> 0  
    THEN 1 ELSE 0 END AS RGU_TV,
    CASE WHEN cardinality(pd_bb_prod_nm) <> 0 
    THEN 1 ELSE 0 END AS RGU_BB,
    
    fi_tot_mrc_qty ,
    fi_tot_mrc_qty as invol_rgu_churn,
    
    case 
    when case when fi_vo_mrc_qty is null then 0 else fi_vo_mrc_qty end + case when fi_tv_mrc_qty is null then 0 else fi_tv_mrc_qty end + case when fi_bb_mrc_qty is null then 0 else fi_bb_mrc_qty end = 1 then '1P'
    when case when fi_vo_mrc_qty is null then 0 else fi_vo_mrc_qty end + case when fi_tv_mrc_qty is null then 0 else fi_tv_mrc_qty end + case when fi_bb_mrc_qty is null then 0 else fi_bb_mrc_qty end  = 2 then '2P'
    when case when fi_vo_mrc_qty is null then 0 else fi_vo_mrc_qty end + case when fi_tv_mrc_qty is null then 0 else fi_tv_mrc_qty end + case when fi_bb_mrc_qty is null then 0 else fi_bb_mrc_qty end  = 3 then '3P'
    else null end
    as mix,
    
    pd_bb_tech,
    CASE WHEN (cardinality(filter(pd_bb_prod_nm ,x -> x like '%FTTH%'))<>0) OR 
    (cardinality(filter(pd_tv_prod_nm,x->x like 'NextGen TV'))<>0 and cardinality(pd_bb_prod_nm)=0) OR 
    cardinality(filter(pd_vo_prod_nm,x->x like'%FTTH%'))<>0 THEN 'FTTH'
    ELSE 'HFC' END AS TechFlag,
    --first_value(n_mora) over(partition by act_acct_cd,date_trunc('month',date(a.dt)) order by date(a.dt) desc) as Last_Overdue,
    case 
    when date(a.dt) < date('2023-06-01') then fi_overdue_age +20
    else  fi_outst_age end  as new_mora

FROM "lla_cco_int_ext_dev"."cr_dna_fixed_user_table" a 

where act_cust_type_grp <>'B2B' and (act_acct_stat ='ACTIVO' or act_acct_stat ='SUSPENDIDO')
)

/*
-- ,productos_churners as(
-- select *,sum(coalesce(vo,0)+coalesce(bb,0)+coalesce(tv,0)) as invol_rgu_churn
-- From(select distinct date_trunc('Month',date(dt)) as rgu_month,act_acct_cd,
-- case when min(pd_vo_prod_nm) is not null then 1 end as vo,
-- case when min(pd_bb_prod_nm) is not null then 1 end as bb,
-- case when min(pd_tv_prod_nm) is not null then 1 end as tv
-- from usefulfields
-- group by 1,2
-- )
-- group by 1,2,3,4,5
-- )

-- ,mora_fix_join as(
-- select a.*,invol_rgu_churn 
-- from UsefulFields a left join productos_churners b 
-- on a.act_acct_cd=b.act_acct_cd and rgu_Month=date_trunc('Month',date(a.dt))
-- order by act_acct_cd,dt
-- )
*/

,CustomerBase_BOM AS(
SELECT DISTINCT 
    DATE_TRUNC('MONTH', DATE(dt)) + INTERVAL '1' MONTH  AS Month,
    act_acct_cd AS AccountBOM,
    dt AS b_date,
    act_contact_phone_1 as b_phone,
    pd_vo_prod_nm as b_vo_nm, pd_tv_prod_nm AS b_tv_nm, pd_bb_prod_nm as b_bb_nm, 
    RGU_VO as b_rgu_vo, RGU_TV as b_rgu_tv, RGU_BB AS b_rgu_bb,
    --fi_outst_age as b_overdue,
    fi_overdue_age as b_overdue,
    customer_age as b_tenure, 
    MinInst as b_min_inst,MaxInst as b_max_inst,
    MIX AS b_mix,
    --antiguo:
    -- (coalesce(RGU_VO,0) + coalesce(RGU_TV,0) + coalesce(RGU_BB,0)) AS b_num_rgus,
    ---nuevo:
    fi_tot_mrc_qty as b_num_rgus,
    TechFlag as b_tech_flag, 
    new_mora AS b_mora, 
    avgmrc as b_avg_mrc,
    ACT_CUST_STRT_DT AS b_act_cust_strt_dt,
    
    CASE 
    WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) THEN 'VO'
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV'
    WHEN (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV+VO'
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV'
    WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB+VO'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV+VO' END AS b_bundle_name
    
    -- digital_rgu as digital_rgu

    FROM UsefulFields 
        WHERE date(dt) = date_trunc('MONTH',date(dt)) + interval '1' month  - interval '1' day
        and 
        (new_mora < (select InvoluntaryChurnDays from parameters ) or new_mora is null ) --or--OR date_diff('day',date(oldest_unpaid_bill_dt),date(dt)) < 90)
        -- (fi_overdue_age < (select InvoluntaryChurnDays from parameters ) or fi_overdue_age is null)
)

,CustomerBase_EOM AS(
SELECT DISTINCT 
    DATE_TRUNC('MONTH', DATE(dt)) AS Month, 
    act_acct_cd as AccountEOM,
    dt as e_date,
    act_contact_phone_1 as e_phone, 
    pd_vo_prod_nm as e_vo_nm, pd_tv_prod_nm as e_tv_nm, pd_bb_prod_nm as e_bb_nm,
    RGU_VO as e_rgu_vo, RGU_TV as e_rgu_tv, RGU_BB AS e_rgu_bb, 
    --fi_outst_age as e_overdue, 
    fi_overdue_age as e_overdue,
    customer_age as e_tenure,
    MinInst as e_min_inst,MaxInst as e_max_inst, MIX AS e_mix,
    -- antiguo:
    -- (coalesce(RGU_VO,0) + coalesce(RGU_TV,0) + coalesce(RGU_BB,0)) AS e_num_rgus,
    -- nuevo:
    fi_tot_mrc_qty as e_num_rgus,
    TechFlag as e_tech_flag,
    new_mora AS e_mora,
    avgmrc as e_avg_mrc, 
    ACT_CUST_STRT_DT AS e_act_cust_strt_dt,

    CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) THEN 'VO'
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV'
    WHEN (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV+VO'
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV'
    WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB+VO'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV+VO' END AS e_bundle_name
    
    -- digital_rgu as e_digital_rgu

    FROM UsefulFields
    WHERE date(dt) = date_trunc('MONTH',date(dt)) + interval '1' month - interval '1' day 
    --and (fi_outst_age <= 90 or fi_outst_age is null)
    and 
    (new_mora <= (select InvoluntaryChurnDays from parameters ) or new_mora is null) --or --OR date_diff('day',date(oldest_unpaid_bill_dt),date(dt)) <= 90)
    --(fi_overdue_age < (select InvoluntaryChurnDays from parameters ) or fi_overdue_age is null)
)

,FixedCustomerBase AS(
    SELECT DISTINCT
    -- Fixed_month:
    CASE 
    WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.Month
    WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.Month
    END AS fixed_month,
    
    -- Fixed_account
    CASE 
    WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN accountBOM
    WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN accountEOM
    END AS fixed_account,
    
    -- Active BOM & EOM
   CASE WHEN accountBOM IS NOT NULL THEN 1 ELSE 0 END AS active_bom,
   CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS active_eom,
   
   -- BOM info: 
   b_phone,b_date,
  b_vo_nm, b_tv_nm, b_bb_nm,
   b_rgu_vo, b_rgu_tv, b_rgu_bb, b_num_rgus, b_overdue, b_tenure, b_min_inst,b_max_inst, b_bundle_name,b_mix, 
  b_tech_flag, 
   case when b_mora is null then 0 else b_mora end as b_mora, b_avg_mrc, b_act_cust_strt_dt,
   
   -- EOM info: 
   e_phone,e_date, 
  e_vo_nm, e_tv_nm, e_bb_nm, 
   e_rgu_vo, e_rgu_tv, e_rgu_bb, e_num_rgus, e_overdue, e_tenure, e_min_inst,e_max_inst, e_bundle_name,e_mix,
  e_tech_flag,
   case when e_mora is null then 0 else e_mora end as e_mora, e_avg_mrc, e_act_cust_strt_dt
   
  ,case when e_avg_mrc - b_avg_mrc <0 then 'downspin' else 'upspin' end as spin
   
  FROM CustomerBase_BOM b FULL OUTER JOIN CustomerBase_EOM e ON b.AccountBOM = e.AccountEOM AND b.Month = e.Month
)

--------------------------------------Main Movements------------------------------------------
,migraciones as (
select
DISTINCT 
a.cve_contrato
from
(select cve_contrato,tipo_parque  from "db-stage-dev"."cr_dna_fixed_isoft" where date(dt) = (select input_month from parameters) - interval '1' day) a
full outer join 
(select cve_contrato,tipo_parque  from "db-stage-dev"."cr_dna_fixed_isoft" where date(dt) = (select input_month from parameters) + interval '1' month - interval '1' day) b
on a.cve_contrato = b.cve_contrato
where a.tipo_parque <> 'B2B' and b.tipo_parque = 'B2B'
)

,MAINMOVEMENTBASE AS(
 SELECT f.*, CASE
 WHEN (e_num_rgus - b_num_rgus)=0 THEN '01. Same RGUs'
 WHEN (e_num_rgus - b_num_rgus)>0 THEN '02. Upsell'
 WHEN (e_num_rgus - b_num_rgus)<0 then '03. Downsell'
 WHEN (b_num_rgus IS NULL AND e_num_rgus > 0 AND DATE_TRUNC ('MONTH', e_act_cust_strt_dt) <> Fixed_Month) 
--  AND date_diff('month',e_max_inst,cast(Fixed_Month as timestamp))>=1
 THEN '04. Come Back to Life'
 WHEN (b_num_rgus IS NULL AND e_num_rgus > 0 AND date_trunc('MONTH',e_act_cust_strt_dt) = cast(Fixed_Month as date))
 THEN '05. New Customer'
 when active_bom = 1 AND active_eom = 0 and b.cve_contrato is not null then '07.B2C to B2B Adjustment'
 WHEN active_bom = 1 AND active_eom = 0 THEN '06. Loss'
 
--  WHEN (b_num_rgus IS NULL AND e_num_rgus > 0 AND DATE_TRUNC ('MONTH', e_act_cust_strt_dt) <> Fixed_Month) Then '07. Missing Customer'
 END AS main_movement, e_num_rgus - b_num_rgus as dif_total_rgu
 FROM FixedCustomerBase f 
 left join migraciones b on f.fixed_account = b.cve_contrato
)

,spin_movementBASE AS (
    SELECT b.*,
    CASE
    WHEN b_tenure/30 <=6 THEN 'Early Tenure'
    WHEN (b_tenure/30 >6 and b_tenure/30 <= 12)  THEN 'Mid Tenure'
    when b_tenure/30 > 12 then 'Late Tenure'
    ELSE NULL END AS b_fixed_tenure_segment,
    
    CASE
    WHEN e_tenure/30 <=6 THEN 'Early Tenure'
    WHEN (e_tenure/30 >6 and e_tenure/30 <= 12)  THEN 'Mid Tenure'
    WHEN e_tenure/30 > 12 then 'Late Tenure'
    ELSE NULL END AS e_fixed_tenure_segment,
    
    
    CASE 
    WHEN main_movement='01. Same RGUs' AND (e_avg_mrc - b_avg_mrc) > 0 THEN '1. Up-spin' 
    WHEN main_movement='01. Same RGUs' AND (e_avg_mrc - b_avg_mrc) < 0 THEN '2. Down-spin' 
    ELSE '3. No Spin' END AS spin_movement
    FROM MAINMOVEMENTBASE b
)

-------------------------------------------- Churn candidates -----------------------------------------------------------

,InactiveUsers as(
Select distinct 
Fixed_Month,
Fixed_Account,
case when fixed_account is not null THEN '1. Fixed Voluntary Churner'
Else Null End as VolChurners,b_num_rgus,b_mora,b_date,e_date,b_overdue
From spin_movementBASE
WHERE active_bom=1 and (active_eom=0 or active_eom is null) 
) 

,service_orders as (
select 
distinct 
date_trunc('MONTH',date(order_start_date)) as month ,
account_name
from "db-stage-prod-lf"."so_cr" 
where order_type = 'DESINSTALACION' --and order_status = 'FINALIZADA'  
and  date_trunc('MONTH',date(order_start_date)) = (select input_month from parameters)
)

,voluntary_churners as (
select 
distinct fixed_month,
fixed_account as voluntary,
b_num_rgus as vol_rgus,
row_number()over(partition by fixed_account) as num 
from inactiveusers inner join service_orders on cast(fixed_account as varchar) = account_name and fixed_month = month
)

-- ,involutary_candidates as (
-- select
-- distinct fixed_month,
-- fixed_account as involuntary,
-- b_num_rgus as invol_rgus,
-- b_mora
-- from inactiveusers 
-- where   b_mora  >= (select InvoluntaryChurnDays from parameters) - date_diff('day',date(b_date),date(b_date)+interval '1' month)-- or
--         -- b_overdue >= (select InvoluntaryChurnDays from parameters) - date_diff('day',date(b_date),date(b_date)+interval '1' month)

-- )

,reached_threshold as (
select distinct 
(date_trunc('MONTH',date(dt))) as month
,act_acct_cd
,max(new_mora) as max_mora
from usefulfields 
where act_acct_cd in (select fixed_account from inactiveusers)
group by 1,2
)

,involuntary_candidates as (
select
distinct fixed_month,
fixed_account as involuntary,
b_num_rgus as invol_rgus
from inactiveusers 
inner join reached_threshold 
on act_acct_cd = fixed_account and month = fixed_month
where max_mora >= (select InvoluntaryChurnDays from parameters)
-- where   b_mora  >= (select InvoluntaryChurnDays from parameters) - date_diff('day',date(b_date),date(b_date)+interval '1' month)-- or
        -- b_overdue >= (select InvoluntaryChurnDays from parameters) - date_diff('day',date(b_date),date(b_date)+interval '1' month)
)

,transfers_out as (
select distinct account_name  from "lla_cco_int_san"."so_temp"
where date_trunc('MONTH',date(cast(completed_date as timestamp))) = (select input_month from parameters)
and order_type = 'DESINSTALACION' 
and (command_id in ('TRAMITE INTERNO','DESINSTALACION POR TX') or command_id like 'MIGRACION%')
)

,transfers_in as (
select distinct account_name,command_id  from "lla_cco_int_san"."so_temp"
where date_trunc('MONTH',date(cast(completed_date as timestamp))) =  (select input_month from parameters) 
and order_type = 'INSTALACION' 
and (command_id in ('CAMBIO NOMBRE','INSTALACION POR TX','TRAMITE INTERNO'))
)

,early_dx as (
select distinct account_name  from "lla_cco_int_san"."so_temp"
where date_trunc('MONTH',date(order_start_date)) = (select input_month from parameters)
and order_type = 'DESINSTALACION' 
and command_id in ( 'MOROSIDAD','MOROSIDAD CABLETICA')
)

,full_churners as (
select  
case when i.fixed_month is null then v.fixed_month else i.fixed_month end as fixed_month,
case when voluntary is null and involuntary is not null then involuntary
    when voluntary is not null and involuntary is null then voluntary
    when voluntary is not null and involuntary is not null then involuntary
else null  end as account
,voluntary, involuntary,
case when voluntary is null and involuntary is not null then '2. Fixed Involuntary Churner' 
    when voluntary is not null and involuntary is null then '1. Fixed Voluntary Churner'
    when voluntary is not null and involuntary is not null then '2. Fixed Involuntary Churner'

else null  end as invol_vol_flag
from involuntary_candidates i full outer join voluntary_churners v on involuntary = voluntary and i.fixed_month = v.fixed_month
)

,final_churn_flag as (
select distinct 
a.fixed_month as ChurnMonth, fixed_account as churn_account,
case when invol_vol_flag is null then '1. Fixed Voluntary Churner' 
when fixed_account in (select cast(account_name as varchar) from early_dx) then '3. Early Disconnection'
else invol_vol_flag end as fixed_churner_type,
b_num_rgus as rgus_churned
from inactiveusers a 
left join full_churners b on account = fixed_account and a.fixed_month = b.fixed_month
)

/*
,max_rgus as( 
select distinct act_acct_cd,sum(vo+bb+tv) as max_rgus_count from(
select distinct act_acct_cd,
case when first_value (pd_vo_prod_nm) over(PARTITION  BY act_acct_cd ORDER BY dt) is not null then 1 else 0 end as vo,
case when first_value (pd_bb_prod_nm) over(PARTITION  BY act_acct_cd ORDER BY dt) is not null then 1 else 0 end as bb,
case when first_value (pd_tv_prod_nm) over(PARTITION  BY act_acct_cd ORDER BY dt) is not null then 1 else 0 end as tv
FROM usefulfields
) group by 1
)

,max_rgus_inactive_users as(
select a.*,max_rgus_count from InactiveUsers a left join max_rgus b on fixed_account=act_acct_cd
)

,FIRSTCUSTRECORD AS (

    SELECT DATE_TRUNC('MONTH',Date_add('MONTH',1, DATE(dt))) AS MES,
    act_acct_cd AS Account,
    min(date(dt)) AS FirstCustRecord,
    date_add('day',-1,min(date(dt))) as PrevFirstCustRecord
    FROM usefulfields
    --WHERE CAST(mora_fix as INT) < (select InvoluntaryChurnDays From parameters)
    WHERE date(dt) = date_trunc('MONTH', DATE(dt)) + interval '1' MONTH - interval '1' day
    Group by 1,2
)

,LastCustRecord as(
    SELECT  DATE_TRUNC('MONTH', DATE(dt)) AS MES,
    act_acct_cd AS Account, 
    max(date(dt)) as LastCustRecord,
    date_add('day',-1,max(date(dt))) as PrevLastCustRecord,
    date_add('day',-2,max(date(dt))) as PrevLastCustRecord2
    
    FROM usefulfields
      --WHERE DATE(LOAD_dt) = date_trunc('MONTH', DATE(LOAD_dt)) + interval '1' MONTH - interval '1' day
   Group by 1,2
   --order by 1,2
)

,NO_OVERDUE AS(
 SELECT DISTINCT 
 
 DATE_TRUNC('MONTH',Date_add('MONTH',1, DATE(dt))) AS MES,
 act_acct_cd AS Account, 
 new_mora
 
 FROM usefulfields t
 INNER JOIN FIRSTCUSTRECORD  r ON r.account = t.act_acct_cd
 WHERE (CAST(new_mora as INT) < (select InvoluntaryChurnDays From parameters) or new mora is null)
 
-------------------------------------------------------------------------------------------------------------------------- 
---- LINEA QUE SE AGREGA CON EL FIN DE MANEJAR LA EXCEPCIÓN DE QUE SE QUEDÓ PEGADA LA FI OUST AGE UN DÍA -----------------
--  OR  date_diff('day',date(oldest_unpaid_bill_dt),date(dt)) < 90
--------------------------------------------------------------------------------------------------------------------------

    and (date(t.dt) = r.FirstCustRecord or date(t.dt)=r.PrevFirstCustRecord)
 GROUP BY 1, 2, 3
)

,OVERDUELASTDAY AS(
 SELECT DISTINCT
 DATE_TRUNC('MONTH', DATE(dt)) AS MES,
 act_acct_cd AS Account,
 new_mora,
 invol_rgu_churn,
 (date_diff('DAY',MaxInst,DATE(dt))) as ChurnTenureDays
 FROM usefulfields  t
 INNER JOIN LastCustRecord r ON date(t.dt) = r.LastCustRecord and 
 r.account = t.act_acct_cd
 WHERE (date(t.dt)=r.LastCustRecord or date(t.dt)=r.PrevLastCustRecord or date(t.dt)=r.PrevLastCustRecord2)
    and CAST(new_mora AS INTEGER) >= (select InvoluntaryChurnDays From parameters)

------------------------------------------------------------------------------------------------------------------------    
-- LINEA QUE SE AGREGA CON EL FIN DE MANEJAR LA EXCEPCIÓN DE QUE SE QUEDÓ PEGADA LA FI OUST AGE UN DÍA -----------------
--  OR  date_diff('day',date(oldest_unpaid_bill_dt),date(dt)) >= 90
------------------------------------------------------------------------------------------------------------------------
 GROUP BY 1,2,3,4,5
 )
 
,INVOLUNTARYNETCHURNERS AS(
 SELECT DISTINCT 
 n.MES AS Month,
 n. account,
 l.ChurnTenureDays,
 invol_rgu_churn
 FROM NO_OVERDUE n INNER JOIN OVERDUELASTDAY l ON n.account = l.account and n.MES = l.MES
 )

,InvoluntaryChurners AS(
SELECT DISTINCT i.Month, i.Account AS ChurnAccount, i.ChurnTenureDays,i.invol_rgu_churn
,CASE WHEN i.Account IS NOT NULL THEN '2. Fixed Involuntary Churner' END AS InvolChurner
FROM INVOLUNTARYNETCHURNERS i left join usefulfields f on i.account=f.act_acct_cd and i.month=date_trunc('month',date(f.dt))
where last_overdue>=(select InvoluntaryChurnDays From parameters)

-------------------------------------------------------------------------------------------------------------------------
--- LINEA QUE SE AGREGA CON EL FIN DE MANEJAR LA EXCEPCIÓN DE QUE SE QUEDÓ PEGADA LA FI OUST AGE UN DÍA -----------------
--  OR  date_diff('day',date(oldest_unpaid_bill_dt),date(dt)) >= 90
 ------------------------------------------------------------------------------------------------------------------------
GROUP BY 1, Account,4, ChurnTenureDays
)

,FinalInvoluntaryChurners AS(
    SELECT DISTINCT MONTH, ChurnAccount,InvolChurner,invol_rgu_churn
    FROM InvoluntaryChurners
    WHERE InvolChurner = '2. Fixed Involuntary Churner'
)

,AllChurners AS(
SELECT f.*,b.* From max_rgus_inactive_users f Full Outer Join FinalInvoluntaryChurners b
ON Fixed_month=Month and ChurnAccount=Fixed_Account
)

,FinalFixedChurners as(
select 
case when Month is not null THEN Month else fixed_Month End as ChurnMonth,
case when ChurnAccount is not null THEN ChurnAccount else Fixed_Account End as Churn_Account,
case 
when InvolChurner is not null and VolChurners is not null then InvolChurner
when InvolChurner is not null then InvolChurner
when VolChurners  is not null then VolChurners 
end as fixed_churner_type,
invol_rgu_churn,max_rgus_count
From AllChurners
)

*/

,ChurnersFixedTable as(
select f.*, 
case when active_eom=1 then null else fixed_churner_type end as fixed_churner_type,
rgus_churned 
FROM spin_movementBASE f left join final_churn_flag
on Fixed_Month=ChurnMonth and Fixed_Account=Churn_Account
)

,transfers as (
select *,
case 
when (fixed_churner_type = '1. Fixed Voluntary Churner' or main_movement = '03. Downsell') and substr(lpad(cast(fixed_account as varchar),12,'0'),-10) in (select substr(lpad(cast(account_name as varchar),12,'0'),-10) from transfers_out) then '01. Transfer Out' 
WHEN  main_movement IN ('02. Upsell', '05. New Customer') and substr(lpad(cast(fixed_account as varchar),12,'0'),-10) in (select substr(lpad(cast(account_name as varchar),12,'0'),-10) from transfers_in) then '02. Transfer In' 
else null end as transfer_flag
from ChurnersFixedTable
)

-- ,transfers_information as (
-- select distinct cve_contrato, max(transfer) as transfer_flag 
-- from (
-- select 
-- cve_contrato,
-- destipoorden, 
-- case when destipoorden in ('TRASLADO EXTERNO','MIGRACION') then 1 else 0 end as transfer
-- from cr_dna_fixed_isoft where date(dt) between (select input_month from parameters) - interval '1' day and (select input_month from parameters) + interval '1' month - interval '1' day
-- )
-- group by 1
-- )

-- ,transfer_identification as (
-- select c.*,
-- case when main_movement in ( '05. New Customer', '04. Come Back to Life') and transfer_flag = 1 then 'Transfer In'
--  when main_movement = '06. Loss' and fixed_churner_type = '1. Fixed Voluntary Churner' and transfer_flag = 1 then 'Transfer Out' end as Transfer_sub
--  from ChurnersFixedTable c 
--  left join transfers_information a on c.fixed_account = a.cve_contrato
-- )


--------------------------------------------------------------------------- Rejoiners -------------------------------------------------------------
/*
,Inactive_Users as(
Select Distinct Fixed_Month as exit_month,
fixed_account as exit_account,
fixed_churner_type
,date_add('month', 1, Fixed_Month) AS rejoiner_month
From ChurnersFixedTable
Where fixed_churner_type is not null
)

,mora_inactive_users as(
select distinct month,new_mora,act_acct_cd From usefulfields
Where new_mora <=(select InvoluntaryChurnDays From parameters)--or-- or (fi_outst_age is null and date_trunc('Month',lst_pymt_dt)=month)
     --fi_overdue_age <=(select InvoluntaryChurnDays From parameters)
--order by 1,2
)

,Rejoiners as(
Select rejoiner_month,act_acct_cd as fixed_rejoiner,case 
when fixed_churner_type='1. Fixed Voluntary Churner' THEN '1. Fixed Voluntary Rejoiner'
when fixed_churner_type='2. Fixed Involuntary Churner' THEN '2. Fixed Involuntary Rejoiner'
Else null end as fixed_rejoiner_type 
From Inactive_Users a inner join mora_inactive_users b
ON exit_account=act_acct_cd and rejoiner_month=month
--date_diff('month',exit_month,month)<=2 and date_diff('month',exit_month,month)>0
)

,rejoiners_master_table as(
Select distinct a.*,fixed_rejoiner,fixed_rejoiner_type 
From ChurnersFixedTable a 
left join rejoiners 
ON rejoiner_month=fixed_month and fixed_rejoiner=fixed_account
)
*/


,rejoiner_candidates as (
select fixed_month + interval '1' month as  rejoiner_month, fixed_account as rejoiner_account from ChurnersFixedTable
where fixed_month  = (select input_month from parameters) - interval '1' month
and main_movement = '06. Loss'
)

,rejoiners_master_table as (
select f.* , case when rejoiner_account is not null then 'Rejoiner' else null end as rejoiner_flag 
from transfers f left join rejoiner_candidates on fixed_month = rejoiner_month and fixed_account = rejoiner_account
)


-- ,rejoiner_info as (
-- select distinct act_acct_cd,
-- first_value(fi_overdue_age) over (partition by act_acct_cd order by dt ) +20 as first_mora
-- ,first_value(fi_overdue_age) over (partition by act_acct_cd order by dt desc) + 20 as last_mora
-- ,first_value(fi_overdue_age) over (partition by act_acct_cd order by fi_overdue_age desc ) +20 as max_mora
-- from "lla_cco_int_ext_dev"."cr_dna_fixed_user_table"
-- where date(dt) between (select input_month from parameters)  - interval '1' day
-- and (select input_month from parameters) + interval '1' month  - interval '1' day 
-- )

-- ,candidates as (
-- select act_acct_cd,max_mora, first_mora,last_mora from rejoiner_info
-- where 
-- max_mora between 91 and 120 and (last_mora < (select InvoluntaryChurnDays from parameters) or last_mora is null) 
-- )

-- ,rejoiners_master_table as (
-- select f.*
-- , if(fixed_account in (select act_acct_cd from candidates),'Rejoiner',null) as rejoiner_flag
-- from transfers f
-- )

,FinalTable as(
SELECT *,CASE
WHEN fixed_churner_type='2. Fixed Involuntary Churner' then rgus_churned
WHEN fixed_churner_type='1. Fixed Voluntary Churner' THEN rgus_churned
WHEN main_movement='03. Downsell' THEN (b_num_rgus - e_num_rgus)
ELSE 0 END AS fixed_rgu_churn

-- ,CONCAT(coalesce(b_vo_nm,'-'),coalesce(b_tv_nm,'-'),coalesce(b_bb_nm,'-')) AS b_plan
-- ,CONCAT(coalesce(e_vo_nm,'-'),coalesce(e_tv_nm,'-'),coalesce(e_bb_nm,'-')) AS e_plan
FROM rejoiners_master_table
)
select * from finaltable where fixed_month = (select input_month from parameters)



