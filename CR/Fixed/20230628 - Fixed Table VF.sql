-----------------------------------------------------------------------
                    -- VERSION FINAL FIXED TABLE--
-----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "lla_cco_int_san"."cr_fixed_table_jun27_v4"  AS  
WITH 

Parameters as(
Select 
    date('2023-05-01') as input_month,
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
    
    case when pd_vo_prod_nm is not null then 1 else 0 end as rgu_vo,
    case when pd_tv_prod_nm is not null then 1 else 0 end as rgu_tv,
    case when pd_bb_prod_nm is not null then 1 else 0 end as rgu_bb,
    

    fi_tot_mrc_qty ,
    fi_tot_mrc_qty as invol_rgu_churn,
    
    case 
    when case when fi_vo_mrc_qty is null then 0 else fi_vo_mrc_qty end + 
         case when fi_tv_mrc_qty is null then 0 else fi_tv_mrc_qty end + 
         case when fi_bb_mrc_qty is null then 0 else fi_bb_mrc_qty end = 1 then '1P'
         
    when case when fi_vo_mrc_qty is null then 0 else fi_vo_mrc_qty end + 
         case when fi_tv_mrc_qty is null then 0 else fi_tv_mrc_qty end + 
         case when fi_bb_mrc_qty is null then 0 else fi_bb_mrc_qty end  = 2 then '2P'
         
    when case when fi_vo_mrc_qty is null then 0 else fi_vo_mrc_qty end + 
         case when fi_tv_mrc_qty is null then 0 else fi_tv_mrc_qty end + 
         case when fi_bb_mrc_qty is null then 0 else fi_bb_mrc_qty end  = 3 then '3P'
    else null end
    as mix,
    
    pd_bb_tech,
    
    case when pd_bb_prod_nm like '%FTTH%' or pd_tv_prod_nm like'%FTTH%' then 'FTTH'else 'HFC' end as techflag,
    case when date(a.dt) < date('2023-06-01') then fi_overdue_age +20 else  fi_outst_age end  as new_mora
    
    /*
    CASE WHEN (cardinality(filter(pd_bb_prod_nm ,x -> x like '%FTTH%'))<>0) OR 
    (cardinality(filter(pd_tv_prod_nm,x->x like 'NextGen TV'))<>0 and cardinality(pd_bb_prod_nm)=0) OR 
    cardinality(filter(pd_vo_prod_nm,x->x like'%FTTH%'))<>0 THEN 'FTTH'
    ELSE 'HFC' END AS TechFlag,
    --first_value(n_mora) over(partition by act_acct_cd,date_trunc('month',date(a.dt)) order by date(a.dt) desc) as Last_Overdue,
    */
    /*
    CASE WHEN cardinality(pd_vo_prod_nm) <> 0  
    THEN 1 ELSE 0 END AS RGU_VO,
    CASE WHEN cardinality(pd_tv_prod_nm) <> 0  
    THEN 1 ELSE 0 END AS RGU_TV,
    CASE WHEN cardinality(pd_bb_prod_nm) <> 0 
    THEN 1 ELSE 0 END AS RGU_BB,
    */


FROM "lla_cco_int_ext_dev"."cr_dna_fixed_user_table" a 

where act_cust_type_grp <>'B2B' and (act_acct_stat ='ACTIVO' or act_acct_stat ='SUSPENDIDO')
)

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

/*
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

,transfers_info as (
select distinct NUMCONTRATO,
first_value(mov_cdg)over(partition by numcontrato order by cve_dia ) as first_mov,
first_value(mov_cdg)over(partition by numcontrato order by cve_dia desc) as last_mov,
first_value(tipo_movimiento)over(partition by numcontrato order by cve_dia ) as first_tipo_mov,
first_value(tipo_movimiento)over(partition by numcontrato order by cve_dia desc) as last_tipo_mov
from "lla_cco_int_san"."mov_ext_table"
where try(date_trunc('MONTH',date(CONCAT(MES_creacion_orden ,'-01')))) = (select input_month from parameters)
--and mov_cdg = 'CAMBIO PRODUCTO' 
)

,transfers_out as (
select distinct NUMCONTRATO  from transfers_info
where (last_mov =  'CAMBIO PRODUCTO' and last_tipo_mov = 'SALIDAS') or (first_mov =  'CAMBIO PRODUCTO' and first_tipo_mov = 'SALIDAS') 

)

,transfers_in as (
select distinct NUMCONTRATO,last_tipo_mov, first_tipo_mov from transfers_info
where (last_mov =  'CAMBIO PRODUCTO' and last_tipo_mov = 'ENTRADAS') or (first_mov =  'CAMBIO PRODUCTO' and first_tipo_mov = 'ENTRADAS') 
)

,early_dx as (
select distinct account_name  from "lla_cco_int_san"."so_temp"
where date_trunc('MONTH',date(order_start_date)) = (select input_month from parameters)
and order_type = 'DESINSTALACION' 
and command_id in ( 'MOROSIDAD','MOROSIDAD CABLETICA')
)
*/

,movimientos_table as (
select distinct a.numcontrato, 
first_value(mov_cdg) over(partition by a.numcontrato order by cve_dia)  as first_movement,
first_value(tipo_movimiento) over(partition by a.numcontrato order by cve_dia)  as first_movement_type,
first_value(des_movimiento) over(partition by a.numcontrato order by cve_dia)  as first_movement_type_desc,
first_value(mov_cdg) over(partition by a.numcontrato order by cve_dia DESC)  as last_movement,
first_value(tipo_movimiento) over(partition by a.numcontrato order by cve_dia DESC)  as last_movement_type,
first_value(des_movimiento) over(partition by a.numcontrato order by cve_dia DESC)  as last_movement_type_desc,
first_value(date(concat(substring(cast(cve_dia as varchar),1,4),'-',substring(cast(cve_dia as varchar),5,2),'-',substring(cast(cve_dia as varchar),7,2)))) over(partition by a.numcontrato order by cve_dia DESC)  as last_dt,
balance
from "lla_cco_int_san"."mov_ext_table" a
left join 
(select distinct numcontrato,cardinality(filter(movimientos_array,x->x like ('CAMBIO PRODUCTOENTRADAS'))) - cardinality(filter(movimientos_array,x->x like ('CAMBIO PRODUCTOSALIDAS'))) as balance
from (
select numcontrato,array_agg(concat(mov_cdg,tipo_movimiento) order by cve_dia) as movimientos_array from "lla_cco_int_san"."mov_ext_table" group by 1
)) b on a.numcontrato = b.numcontrato
where date(concat(substring(cast(cve_dia as varchar),1,4),'-',substring(cast(cve_dia as varchar),5,2),'-',substring(cast(cve_dia as varchar),7,2))) between (select input_month from parameters) - interval '1' day and (select input_month from parameters) + interval '1' month - interval '1' day 
-- where try(date_trunc('MONTH',date(CONCAT(MES_creacion_orden ,'-01')))) = (select input_month from parameters)
)

,churn_category as (
select distinct last_dt,numcontrato,
case when last_movement = 'BAJAS' and last_movement_type = 'SALIDAS' and last_movement_type_desc in ('Bajas por solicitud voluntaria','No Determinada') then '1. Fixed Voluntary Churner'
    when last_movement = 'BAJAS' and last_movement_type = 'SALIDAS' and last_movement_type_desc in ('Bajas por morosidad','Bajas por Suspensión Morosidad') then '2. Fixed Involuntary Churner'
    else null end as churn_cat
from movimientos_table
)

,churners as (
select fixed_month as ChurnMonth,fixed_account as churn_account, churn_cat
from inactiveusers 
left join churn_category 
on date_trunc('MONTH',last_dt) = fixed_month 
and substr(lpad(cast(numcontrato as varchar),12,'0'),-10)  = substr(lpad(cast(fixed_account as varchar),12,'0'),-10)
where fixed_month = (select input_month from parameters)

)

/*
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
*/

,ChurnersFixedTable as(
select f.*, 
case when active_eom=1 then null else churn_cat end as fixed_churner_type
FROM spin_movementBASE f left join churners
on Fixed_Month=ChurnMonth and Fixed_Account=Churn_Account
)

,transfer_out as (
select numcontrato from movimientos_table where balance < 0
)
,transfer_in as (
select numcontrato from movimientos_table where balance > 0
)



,transfers as (
select *,
case 
when (fixed_churner_type is null and main_movement = '06. Loss')
and substr(lpad(cast(fixed_account as varchar),12,'0'),-10) in
(select substr(lpad(cast(NUMCONTRATO as varchar),12,'0'),-10) from movimientos_table where last_movement = 'CAMBIO PRODUCTO' and last_movement_type = 'SALIDAS') then '01. Transfer Out' 
when (main_movement = '03. Downsell')
and substr(lpad(cast(fixed_account as varchar),12,'0'),-10) in
(select substr(lpad(cast(NUMCONTRATO as varchar),12,'0'),-10) from movimientos_table where last_movement = 'CAMBIO PRODUCTO' and last_movement_type = 'SALIDAS')
and substr(lpad(cast(fixed_account as varchar),12,'0'),-10) in
(select substr(lpad(cast(NUMCONTRATO as varchar),12,'0'),-10) from transfer_out ) then '01. Transfer Out' 
when (main_movement = '05. New Customer')
and substr(lpad(cast(fixed_account as varchar),12,'0'),-10) in
(select substr(lpad(cast(NUMCONTRATO as varchar),12,'0'),-10) from movimientos_table where first_movement = 'CAMBIO PRODUCTO' and first_movement_type = 'ENTRADAS') then '02. Transfer In' 
when (main_movement = '02. Upsell')
-- and substr(lpad(cast(fixed_account as varchar),12,'0'),-10) in
-- (select substr(lpad(cast(NUMCONTRATO as varchar),12,'0'),-10) from movimientos_table where last_movement = 'CAMBIO PRODUCTO' and last_movement_type = 'ENTRADAS')
and substr(lpad(cast(fixed_account as varchar),12,'0'),-10) in
(select substr(lpad(cast(NUMCONTRATO as varchar),12,'0'),-10) from transfer_in ) then '02. Transfer In' 
else null end as transfer_flag
from ChurnersFixedTable
)

,rejoiner_candidates as (
select fixed_month + interval '1' month as  rejoiner_month, fixed_account as rejoiner_account from inactiveusers
where fixed_month  = (select input_month from parameters) - interval '1' month
-- and main_movement = '06. Loss'
)

,rejoiners_master_table as (
select f.* , case when rejoiner_account is not null then 'Rejoiner' else null end as rejoiner_flag 
from transfers f left join rejoiner_candidates on fixed_month = rejoiner_month and fixed_account = rejoiner_account
)

,FinalTable as(
SELECT *
-- ,CASE
-- WHEN fixed_churner_type='2. Fixed Involuntary Churner' then rgus_churned
-- WHEN fixed_churner_type='1. Fixed Voluntary Churner' THEN rgus_churned
-- WHEN main_movement='03. Downsell' THEN (b_num_rgus - e_num_rgus)
-- ELSE 0 END AS fixed_rgu_churn

-- ,CONCAT(coalesce(b_vo_nm,'-'),coalesce(b_tv_nm,'-'),coalesce(b_bb_nm,'-')) AS b_plan
-- ,CONCAT(coalesce(e_vo_nm,'-'),coalesce(e_tv_nm,'-'),coalesce(e_bb_nm,'-')) AS e_plan
FROM rejoiners_master_table
)


select * from finaltable where fixed_month = (select input_month from parameters)

select FIXED_MONTH,
        ACTIVE_BOM,
        ACTIVE_EOM,
        main_movement,
        spin_movement,
        fixed_churner_type,
        transfer_flag,
        rejoiner_flag,
        COUNT(DISTINCT FIXED_ACCOUNT) AS  ACCOUNTS,
        SUM(B_NUM_RGUS) AS B_RGUS,
        SUM(E_NUM_RGUS) AS E_RGUS
 
from "lla_cco_int_san"."cr_fixed_table_jun27_v4" 
--where fixed_month = (select input_month from parameters) --and date('2023-05-01') 
-- where fixed_month = (select input_month from parameters)
group by 1,2,3,4,5,6,7,8
order by 1,4,5,6,7,8



