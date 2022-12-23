------------------------------------------ SPRINT 3--------------------------------------------------------------

--CREATE TABLE IF NOT EXISTS "lla_cco_int_san"."cr_operational_drivers_prod"  AS

WITH

Parameters AS (
SELECT
    DATE('2022-09-01') AS start_date,
    DATE('2022-11-01') AS end_date
)

,Fmc_Table AS (
SELECT DISTINCT *, 
CASE WHEN B_Plan_Full=E_Plan_Full
--WHEN B_Plan_Full IS NULL AND E_Plan_Full IS NULL AND 
THEN Fixed_Account ELSE NULL END AS no_plan_change_flag
FROM "lla_cco_int_san"."cr_fmc_table"
where month >= (select start_date from Parameters) and month <= (select end_date from Parameters)
)
/* Ajuste a la fi_outst_age: para eliminar saltos en los dias de mora se encuentran los dias en donde se dieron saltos inesperados, se encuentra la mora del dia anterior y del dia siguiente, en caso que el salto en los dias sea mayor/menor a dos dias frente a la mora del dia anterior se remplaza la mora de ese dia por el numero consecutivo a la mora del dia anterior
*/
,mora_error as(
select distinct Month,dt,act_acct_cd,mora,prev_mora,next_mora,Bill_DT_M0,
Oldest_Unpaid_Bill_DT,act_cust_strt_dt
,case when ( (mora-prev_mora)>2 and (mora-next_mora)>2 ) or ( (mora-prev_mora)<-2 and (mora-next_mora)<-2 ) then 1 else 0 end as mora_salto
from(
select distinct date_trunc('Month',date(dt)) as Month,Fi_Outst_Age as mora,dt, act_acct_cd,Bill_DT_M0,
Oldest_Unpaid_Bill_DT,act_cust_strt_dt
,lag(fi_outst_age) over(partition by act_acct_cd order by dt desc) as next_mora
,lag(fi_outst_age) over(partition by act_acct_cd order by dt) as prev_mora
FROM "db-analytics-dev"."dna_fixed_cr"
Where (act_cust_typ='RESIDENCIAL' or act_cust_typ='PROGRAMA HOGARES CONECTADOS') and (act_acct_stat='ACTIVO' or act_acct_stat='SUSPENDIDO')
)
--order by act_acct_cd,dt
)
,mora_arreglada as(
select distinct *
,case when mora_salto=1 then prev_mora+1 
when mora is null and next_mora=prev_mora+2 then prev_mora+1 
else mora end as mora_fix
from mora_error
--order by 3,2
)
--where month=date('2022-09-01') and final_eom_activeflag=1
/* Sales bajo la lógica anterio cambio:
 - Las ventas del mes se calculan a partir de las service orders, se considera una venta del mes si se completo una orden de instalacion
 - Se cruza la service orders con el dna para poder filtrar por el tipo de cuenta y el estado de la cuenta. 
,total_installs as(
select 
date_trunc('Month',act_cust_strt_dt) as Sales_Month,
date_trunc('Month',date(act_acct_inst_dt)) as Install_Month,
act_acct_cd
From "db-analytics-dev"."dna_fixed_cr"
Where (act_cust_typ='RESIDENCIAL' or act_cust_typ='PROGRAMA HOGARES CONECTADOS') and (act_acct_stat='ACTIVO' or act_acct_stat='SUSPENDIDO')
--where act_acct_cd='1366243'
)

,installs_fmc_table as(
select f.*,Sales_Month,Install_Month
From FmcTable f left join total_installs b
ON b.act_acct_cd=Fixed_Account and cast(Month as varchar)=cast(Install_Month as varchar)
)

,Sales_data as(
select distinct date_trunc('Month',act_acct_inst_dt) as Install,first_value(date(dt)) over(partition by act_acct_cd order by dt) as first_dna_date,act_acct_cd --,count(distinct act_acct_cd)
From "db-analytics-dev"."dna_fixed_cr" 
Where (act_cust_typ='RESIDENCIAL' or act_cust_typ='PROGRAMA HOGARES CONECTADOS') and (act_acct_stat='ACTIVO' or act_acct_stat='SUSPENDIDO')
)

,One_sale as (
select *, CASE
WHEN Install>=date_trunc('Month',first_dna_date) THEN Install
ELSE date_trunc('Month',first_dna_date) END AS Sale_date
FROM Sales_data
)

,sales_fmc_table as(
Select f.*, b.act_acct_cd as monthsale_flag 
From installs_fmc_table f left join One_sale b
ON act_acct_cd=Fixed_Account and Month=Sale_date
)
*/

----- Sales : a partir de las service orders----------------------------------------
,SO_INFO as(
    SELECT DISTINCT SC.account_name,
                    FIRST_VALUE(order_id) OVER(PARTITION BY SC.account_name ORDER BY completed_date ASC) AS FI_order,
                    FIRST_VALUE(DATE(order_start_date)) OVER(PARTITION BY SC.account_name ORDER BY completed_date asc) AS FI_order_start_date,
                    FIRST_VALUE(DATE(completed_date)) OVER(PARTITION BY SC.account_name ORDER BY completed_date ASC) as FI_order_completed_date
    FROM "db-stage-dev"."so_cr" as sc
    WHERE
    order_type = 'INSTALACION' 
    AND order_status = 'FINALIZADA'

)

,Sales_data as(
select distinct 
        act_acct_cd AS account_name_1
        ,FIRST_VALUE(DATE(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY act_acct_inst_dt ASC)  as Install
        ,FIRST_VALUE(date(dt)) over(partition by act_acct_cd order by dt ASC) as first_dna_date 
From "db-analytics-dev"."dna_fixed_cr" 
Where (act_cust_typ='RESIDENCIAL' or act_cust_typ='PROGRAMA HOGARES CONECTADOS') and (act_acct_stat='ACTIVO' or act_acct_stat='SUSPENDIDO')

)
,INFO_QUALITY AS (
SELECT
        DATE_TRUNC('MONTH',FI_order_completed_date ) AS reference_month
        ,SO.*
        ,SD.* 
        ,CASE
            -- WHEN DATE_TRUNC('MONTH',FI_order_completed_date ) > DATE_TRUNC('MONTH',IF(first_dna_date > Install, Install, first_dna_date)) THEN 'Install con delay'
            WHEN DATE_TRUNC('MONTH',FI_order_completed_date ) < DATE_TRUNC('MONTH',IF(first_dna_date > Install, Install, first_dna_date)) THEN 'DNA data delayed'
            ELSE 'Mismo mes'
        END AS tipo_venta

FROM SO_INFO AS SO
/*Con este join al DNA tenemos una limitante y es que la única forma que tengo de reconocer, hasta ahora, que una cuenta es tipo residenciales viene desde el DNA... Necesitamos averiguar cómo poderlo determinar desde la base de SO para filtrar desde allá*/
INNER JOIN Sales_data AS SD ON SO.account_name=SD.account_name_1
WHERE DATE_TRUNC('MONTH',FI_order_start_date ) BETWEEN (SELECT start_date FROM Parameters) AND (SELECT end_date FROM Parameters)
    AND DATE_TRUNC('MONTH',FI_order_completed_date ) <= DATE_TRUNC('MONTH',IF(first_dna_date > Install, Install, first_dna_date))
)


,sales_fmc_table as(
Select distinct f.*, account_name as monthsale_flag 
From Fmc_Table f left join  INFO_QUALITY  b
ON fixed_account = account_name and month = reference_month
)

--select distinct month,count(distinct monthsale_flag) from sales_fmc_table group by 1


---------------------------------------- Soft Dx & Never Paid ------------------------------

/* Version anterior de Soft Dx cambio:
    - se cambia la base sobre la cual se calculan los soft_dx, se tienen en consideracion la camada de clientes que entraron al dna el dia con mayor densidad de nuevos usuarios en el dna 
,FirstBill as(
    Select Distinct act_acct_cd as ContratoFirstBill,Min(Bill_DT_M0) as FirstBillEmitted
    From mora_arreglada
    group by 1
)

,Prueba as(
    Select distinct Date_Trunc('Month',cast(dt as date)),act_acct_cd,OLDEST_UNPAID_BILL_DT,mora_fix,date_trunc('Month',min(act_cust_strt_dt)) as Sales_Month,dt
    From mora_arreglada
    group by 1,2,3,4,6
)

,JoinFirstBill as(
    Select Sales_Month,a.act_acct_cd,mora_fix,dt
    FROM Prueba a inner join FirstBill b
    on ContratoFirstBill=act_acct_cd and date_trunc('Month',FirstBillEmitted)=date_trunc('Month',OLDEST_UNPAID_BILL_DT)
)



,MaxOutstAge as(
    Select Distinct Sales_Month,act_acct_cd,Max(mora_fix) as Outstanding_Days,
    Case when Max(mora_fix)>=26 Then act_acct_cd ELSE NULL END AS SoftDx_Flag,
    Case when Max(mora_fix)>=90 Then act_acct_cd ELSE NULL END AS NeverPaid_Flag
    From JoinFirstBill
    group by 1,2
)
*/

-- Numero de cuentas que aparecen el dna por dia 
,cuentas_fecha as (
SELECT  reference_month
        ,first_dna_date
        ,COUNT(DISTINCT account_name) AS cant_users

FROM INFO_QUALITY
group by 1,2
)

-- Flag del dia con mayor densidad de cuentas que ingresan al dna
,fechas_maxima as(
select reference_month
, first_dna_date
, cant_users
, row_number() over (partition by reference_month order by cant_users desc) as relevante 
from cuentas_fecha
)
-- Camada de usuarios que ingresaron en el dna en los dias de mayor densidad de cuentas, es la base sobre de clientes que se va a tener en cuenta para calcular el soft_dx
,candidatos_soft_dx as (
select a.*,b.first_dna_date
from INFO_QUALITY a 
inner join fechas_maxima b on a.reference_month = b.reference_month and relevante =1
)

,soft_dx_never_paid as (
    select reference_month
    ,account_name
    ,case when max(mora_fix)>= 26 and max(mora_fix)<90 then account_name else null end as Flag_soft_dx
    ,case when max(mora_fix)>= 90  then account_name else null end as Flag_never_paid
     from mora_arreglada a left join candidatos_soft_dx b on act_acct_cd = account_name_1 
     group by 1,2
)


,SoftDx_MasterTable as(
    Select f.*,Flag_soft_dx,Flag_never_paid,reference_month
    From sales_fmc_table  f left join soft_dx_never_paid b ON account_name =Fixed_Account and b.reference_month=Month
)

--select month, base_soft, count(distinct base), count(distinct flag_soft_dx) from SoftDx_MasterTable group by 1 order by 1 
/*
,NeverPaid_MasterTable as(
    Select f.*,Flag_never_paid
    From SoftDx_MasterTable f left join MaxOutstAge b ON act_acct_cd=Fixed_Account and cast(b.Sales_Month as date)=Month
)
*/
--/*
----------------------------------------- Early Interactions ----------------------------------------
-------------------- eliminar interacciones repetidas -----------------------------------------------
,dup_fix AS(
SELECT *, row_number() OVER (PARTITION BY account_id, cast(interaction_start_time AS DATE),interaction_purpose_descrip ORDER BY interaction_start_time DESC) AS row_num
FROM "db-stage-dev"."interactions_cabletica"
WHERE (account_type='RESIDENCIAL' or account_type='PROGRAMA HOGARES CONECTADOS') 
AND date_trunc('Month',interaction_start_time)>=DATE('2022-01-01') and interaction_status <> 'ANULADA' and interaction_status <> 'CANCELADA'
)


,interactions as(
select date_trunc('Month',interaction_start_time) as interaction_month,interaction_start_time,account_id,interaction_end_time FROM (select *from dup_fix where row_num = 1) 
WHERE (account_type='RESIDENCIAL' or account_type='PROGRAMA HOGARES CONECTADOS')  and interaction_status <> 'ANULADA'
and interaction_purpose_descrip NOT IN ('VENTANILLA','DESINSTALACION','CORPORATIVO','VENCIMIENTO PROMOCION','*NO DEFINIDO*','SUSPENSIONES',
'CREACION DE FACTURA','VENTAS')
)
/*
,CustStarts as(
select distinct date_trunc('Month',act_cust_strt_dt) as start_month,act_cust_strt_dt,act_acct_cd,
first_value(date(dt)) over(partition by act_acct_cd order by dt) as first_dna_date 
From "db-analytics-dev"."dna_fixed_cr"
Where (act_cust_typ='RESIDENCIAL' or act_cust_typ='PROGRAMA HOGARES CONECTADOS') and (act_acct_stat='ACTIVO' or act_acct_stat='SUSPENDIDO')
)

,EarlyInt as(
select a.*,b.* FROM interactions a inner join CustStarts b
on account_id=act_acct_cd
WHERE date_diff('day',date(act_cust_strt_dt),date(interaction_start_time))<=21
)
*/
--/*
,EarlyInt as(
select a.*,b.* FROM interactions a inner join INFO_QUALITY b
on account_id=account_name
WHERE date_diff('day',date(reference_month),date(interaction_start_time))<=21
)
--*/

,Early_interaction_MasterTable AS(
  SELECT DISTINCT f.*,c.account_name as EarlyIssue_Flag,Interaction_Month,f.reference_month
  FROM SoftDx_MasterTable f LEFT JOIN EarlyInt c 
  ON Fixed_Account=c.account_name AND date_trunc('Month',c.reference_month)=Month 
) 


 -- select distinct month, count(distinct earlyIssue_flag) from Early_interaction_MasterTable group by 1


-------------------------------------- New users early tech tickets -------------------------------


,tickets as(
select date_trunc('Month',interaction_start_time) as ticket_month,interaction_start_time,account_id,interaction_end_time FROM (select *from dup_fix where row_num = 1)
WHERE date_trunc('Month',interaction_start_time)>=date('2022-01-01') and interaction_status <> 'ANULADA'
and interaction_purpose_descrip IN (
    'AVERIAS',
    'SIN SERVICIO INTERNET',
    'INTERRUPCION CONSTANT SERVICIO',
    'EQUIPO INTEGRADO',
    'PROB VELOCIDAD',
    'SIN SEÑAL',
    'OTRO INTERNET',
    'CONTROL REMOTO',
    'SIN SERVICIO TV',
    'PROB STB DIG',
    'SIN SEÑAL UNO/VARIOS CH DVB',
    'PROB CABLE MODEM',
    'MENSAJE ERROR DVB',
    'PROB STB DVB',
    'MENSAJE ERROR',
    'INTERNET/DIGITAL',
    'CALIDAD SEÑAL',
    'TV/INTERNET/DIGITAL',
    'SIN SERVICIO TODOS LOS CH DVB',
    'SIN SERVICIO TELEFONIA',
    'CONTROL REMOTO DVB',
    'AVERIA',
    'PROB STB',
    'SEÑAL LLUVIOSA/RAYAS TV',
    'CONEXION',
    'TV/INTERNET',
    'FALTAN CH PARRILLA BASICA DVB',
    'NIVELES SEÑAL INCORRECTOS INT.',
    'TV/DIGITAL',
    'SIN SEÑAL UNO/VARIOS CH',
    'SIN SERVICIO INT',
    'PROB ROUTER CT',
    'CALIDAD SEÑAL DVB'
)
)

,TicketStarts as(
select distinct date_trunc('Month',act_cust_strt_dt) as start_month,act_cust_strt_dt,act_acct_cd,
first_value(date(dt)) over(partition by act_acct_cd order by dt) as first_dna_date 
From "db-analytics-dev"."dna_fixed_cr"
WHERE (act_cust_typ='RESIDENCIAL' or act_cust_typ='PROGRAMA HOGARES CONECTADOS') 
)

,EarlyTick as(
select a.*,b.* FROM tickets a inner join TicketStarts b
on account_id=act_acct_cd
WHERE date_diff('week',date(act_cust_strt_dt),date(interaction_start_time))<=7
)

,CallsMasterTable AS (
  SELECT DISTINCT f.*, act_acct_cd as TechCall_Flag FROM Early_interaction_MasterTable f LEFT JOIN EarlyTick c 
  ON Fixed_Account=Account_ID AND Month=date_trunc('Month',first_dna_date)
)

--------------------------------------------- Bill Claims ------------------------------------

,CALLS AS (
SELECT account_id AS CONTRATO, DATE_TRUNC('Month',interaction_start_time) AS Call_Month, Interaction_id
    FROM (select *from dup_fix where row_num = 1)
    WHERE 
        account_id IS NOT NULL
        AND interaction_status <> 'ANULADA'
        AND interaction_purpose_descrip IN ( 
        'INFORMACION',
        'MONTO DE FACTURACION',
        'NOTAS DE CREDITOS',
        'FACTURACION/COBROS',
        'FORMA DE PAGO',
        'CAMBIO DE PRECIO',
        'ANULACION DE FACTURA',
        'COSTO DE SERVICIOS',
        'ENVIO FACTURA',
        'NOTAS DE CREDITO'
)
)
,CallsPerUser AS (
    SELECT DISTINCT CONTRATO, Call_Month, Count(DISTINCT interaction_id) AS NumCalls
    FROM CALLS
    GROUP BY CONTRATO, Call_Month
)

,BillingCallsMasterTable AS (
SELECT DISTINCT F.*, CASE WHEN NumCalls IS NOT NULL THEN CONTRATO ELSE NULL END AS BillClaim_Flag
FROM CallsMasterTable f LEFT JOIN CallsPerUser 
ON Contrato=Fixed_Account AND Call_Month=Month
)

-------------------------------------- Bill Shocks -----------------------------------------

,AbsMRC AS (
SELECT *, abs(mrc_change) AS Abs_MRC_Change FROM BillingCallsMasterTable

)
,BillShocks AS (
SELECT DISTINCT *,
CASE
WHEN Abs_MRC_Change>(b_total_mrc*(.05)) AND B_PLAN=E_PLAN AND no_plan_change_flag is not null THEN Fixed_Account ELSE NULL END AS increase_flag
FROM AbsMRC
)

-------------------------------------- Outlier Installation ---------------------------------


,Installations_6_days as(
SELECT distinct account_name,order_id, date_trunc('Day',order_start_date) as start_date,Date_Trunc('Day',completed_date) as completed_date,
Date_DIFF('Day',order_start_date,completed_date) as Install_Time
FROM "db-stage-dev"."so_cr"
WHERE
order_type = 'INSTALACION' 
AND order_status = 'FINALIZADA'
AND Date_DIFF('Day',order_start_date,completed_date) >5
)


,OutliersMasterTable AS (
    SELECT DISTINCT f.*, b.account_name as long_install_flag
    FROM BillShocks f LEFT JOIN Installations_6_days b ON b.account_name=Fixed_Account AND Month=date_trunc('Month',completed_date)
)

--------------------------------------- Mounting Bills --------------------------------
/*
,BillingInfo as(
  Select distinct date_trunc('Month',cast(dt as date)) as Month,act_acct_cd,OLDEST_UNPAID_BILL_DT,Bill_Dt_M0
  From "db-analytics-dev"."dna_fixed_cr"
 -- WHERE (date(dt) = DATE_TRUNC('Month', date(dt)) and DATE_TRUNC('Month', date(dt))<>date('2022-10-01')) OR dt='2022-10-07'
    WHERE date(dt) between date_trunc('MONTH', date(dt)) and date_add('MONTH',1, date_trunc('MONTH',date(dt)))
    and (act_cust_typ='RESIDENCIAL' or act_cust_typ='PROGRAMA HOGARES CONECTADOS')
)

,MountingBillJoin as(
  Select distinct b.Month as MountingBillMonth,b.act_acct_cd as MountingBill_Flag
  From BillingInfo a inner join BillingInfo b 
  ON a.act_acct_cd=b.act_acct_cd and a.OLDEST_UNPAID_BILL_DT=b.OLDEST_UNPAID_BILL_DT and a.Month=date_add('Month',-1, b.Month)
  Where b.Bill_Dt_M0 IS NOT NULL
)
,MountingBills_MasterTable as(
  Select f.*,MountingBill_Flag From OutliersMasterTable f left join Mounting_Bills
  ON Month=MountingBillMonth and Fixed_Account=MountingBill_Flag
*/

,Mounting_bill_60 as (
select distinct month, act_acct_cd,
case when mora_fix = 60 then 1 else 0 end as day_60
from mora_arreglada
)

,Mounting_Bills as (
select distinct month, act_acct_cd,max(day_60) as MountingBill_Flag
from Mounting_bill_60
group by 1,2 
)

,MountingBills_MasterTable as(
  Select f.*,MountingBill_Flag From OutliersMasterTable f left join Mounting_Bills b
  ON f.Month=b.Month and Fixed_Account=act_acct_cd
)

--------------------------------------- Sales Channel ----------------------------------
,SalesChannel as(
SELECT distinct date_trunc('Month',order_start_date) as mes_venta,account_name,
first_value (channel_desc) over(PARTITION  BY account_name ORDER BY order_start_date) AS sales_channel
FROM "db-stage-dev"."so_cr"
WHERE
order_type = 'INSTALACION' 
AND order_status = 'FINALIZADA'
)

,ChannelsMasterTable AS (
    select distinct f.*, sales_channel from MountingBills_MasterTable f left join SalesChannel s
    on account_name=f.Fixed_Account AND s.mes_venta=Month
)
--*/

/*
,SalesChannel as (
SELECT distinct date_trunc('Month',cast(dt as date)) as ChannelMonth,account_name,channel_desc
FROM "db-stage-dev"."so_cr"
WHERE
order_type = 'INSTALACION' 
AND order_status = 'FINALIZADA'
)
,SalesChannelsInstallations as (
select distinct act_acct_cd, account_name,Install_Month,channel_desc
from total_installs left join SalesChannel
on account_name=act_acct_cd and Install_Month=date_trunc('Month',ChannelMonth)
)
,ChannelsMasterTable AS (
    select distinct f.*, channel_desc from MountingBills_MasterTable f left join SalesChannelsInstallations s
    on account_name=f.Fixed_Account AND s.Install_Month=Month
)
*/



--------------------------------------- Grouped table -----------------------------------
--/*
select distinct Month
/*
        ,b_final_tech_flag
        ,b_fmc_segment
        ,e_final_tech_flag
        ,e_fmc_segment
        ,b_tenure_final_flag
        ,e_tenure_final_flag
        ,b_fixed_tenure_segment
        ,b_fixed_tenure_segment
*/
,count(distinct fixed_account) as activebase, 
count(distinct monthsale_flag) as Sales, count(distinct flag_soft_dx) as Soft_Dx, 
max(cant_users) as base_soft_dx,
count(distinct Flag_never_paid) as NeverPaid, count(distinct long_install_flag) as Long_installs, 
count (distinct increase_flag) as MRC_Change, count (distinct no_plan_change_flag) as NoPlan_Changes,
count(distinct EarlyIssue_Flag) as EarlyIssueCall, count(distinct TechCall_Flag) as TechCalls,
count(distinct BillClaim_Flag) as BillClaim,
sum(MountingBill_Flag) as MountingBills
--,sales_channel
--,sales_Month,Install_Month
from ChannelsMasterTable left join cuentas_fecha b on month = b.reference_month
Where final_churn_flag<>'Fixed Churner' AND final_churn_flag<>'Customer Gap' AND final_churn_flag<>'Full Churner' AND final_churn_flag<>'Churn Exception'
--and sales_month>cast('2022-07-01' as date) and Install_month>cast('2022-07-01' as date)
group by 1--,2,3,4,5,6,7,8,9,b_fmc_type, e_fmc_type --,first_sales_chnl_eom, first_sales_chnl_bom, Last_Sales_CHNL_EOM, Last_Sales_CHNL_BOM , sales_channel,sales_channel_soOrder by 1 desc, 2,3,4
Order by 1 desc, 2,3,4
--*/
