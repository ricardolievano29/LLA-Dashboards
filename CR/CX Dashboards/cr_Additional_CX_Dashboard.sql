--CREATE TABLE IF NOT EXISTS "cr_Additional_CX_Dashboard" AS


WITH

fmc_table as(
  Select *,
  CONCAT(case when B_Plan is null then '' else B_Plan end,Case when cast(Mobile_ActiveBOM as VARCHAR) is null then '' else cast(Mobile_ActiveBOM as VARCHAR) end) AS B_PLAN_ADJ,
   CONCAT(case when E_Plan is null then '' else E_Plan end,Case when cast(Mobile_ActiveEOM as VARCHAR) is null then '' else cast(Mobile_ActiveEOM as VARCHAR) end) AS E_PLAN_ADJ
  From "lla_cco_int_san"."cr_fmc_table"
)

,FinalTablePlanAdj AS (
  SELECT DISTINCT *, 
  CASE WHEN B_PLAN_ADJ=E_PLAN_ADJ THEN Fixed_Account ELSE NULL END AS no_plan_change_flag
  FROM fmc_table

)

-------------------------------------------------- Tech calls per 1k RGU ----------------------------------------------------------------------------

,Tiquetes as(
  Select distinct date_trunc('MONTH', interaction_start_time) as TicketMonth,account_id AS Contrato,count(distinct interaction_id) as TechCalls
  From "db-stage-dev"."interactions_cabletica"
  WHERE interaction_purpose_descrip is not null and account_id is not null and interaction_status <> 'ANULADA' and interaction_purpose_descrip IN (
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
    'CALIDAD SEÑAL DVB')
  group by 1,2

)

,Tiquetes_fmc_Table as(

  Select f.*,TechCalls as TechCall_Flag From FinalTablePlanAdj f left join Tiquetes
  ON Fixed_Account = Contrato and Month=cast(TicketMonth as date)
)

------------------------------------------------------ Care Calls ---------------------------------------------------------------------------------------

,Care_Calls as(
  Select distinct date_trunc('MONTH',interaction_start_time) as CallMonth,account_id AS Contrato,count(distinct interaction_id) as CareCalls
  From "db-stage-dev"."interactions_cabletica"
  WHERE interaction_purpose_descrip is not null and account_id is not null
  and interaction_status <> 'ANULADA' and interaction_purpose_descrip <> 'VENTANILLA' 
  group by 1,2
)

,CareCalls_fmc_Table as (
  Select f.*,CareCalls as CareCall_Flag From Tiquetes_fmc_Table f left join Care_Calls
  ON fixed_account=Contrato and Month=cast(CallMonth as date)
)
------------------------------------------------------- Billing Calls per Bill Variation ------------------------------------------------------------------

,AbsMRC AS (
SELECT *, abs(mrc_change) AS Abs_MRC_Change FROM CareCalls_fmc_Table
)
,BillVariations AS (
SELECT DISTINCT *,
CASE
WHEN Abs_MRC_Change>(TOTAL_B_MRC*(.05)) AND B_PLAN=E_PLAN AND no_plan_change_flag is not null THEN Fixed_Account ELSE NULL END AS BillVariation_flag
FROM AbsMRC

)

,BillVariation_MasterTable as(
  Select distinct f.*,BillVariation_Flag From CareCalls_fmc_Table f left join BillVariations b
  ON f.Fixed_Account=b.Fixed_Account and f.Month=b.Month
  
)

,BillVariation_Calls as(
  Select distinct date_trunc('MONTH',interaction_start_time) as CallMonth,account_id AS Contrato,count(distinct interaction_id) as BillingCalls
  From "db-stage-dev"."interactions_cabletica"
  WHERE interaction_purpose_descrip is not null and account_id is not null AND interaction_status <> 'ANULADA'
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
        'NOTAS DE CREDITO')
  group by 1,2
)

,BillVariationCalls_MasterTable as(
  Select f.*,BillingCalls From BillVariation_MasterTable f left join BillVariation_Calls
  ON Contrato=BillVariation_flag and Month=cast(CallMonth as date)

)

------------------------------------------------------------------------ FTR Billing --------------------------------------------------------------------------------

,BillingCalls as (
  Select distinct * From "db-stage-dev"."interactions_cabletica"  
  WHERE interaction_purpose_descrip is not null and account_id is not null AND interaction_status <> 'ANULADA'
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
        'NOTAS DE CREDITO')
)

,CallsWithoutSolution as(
  Select distinct a.interaction_start_time,a.interaction_id, a.account_id
  From BillingCalls a left join billingCalls b ON a.account_id=b.account_id
  Where date_diff('DAY',b.interaction_start_time,a.interaction_start_time) between 0 and 14 and a.interaction_id<>b.interaction_id
  order by 3,1
)

,MultipleCallsFix as(
  Select distinct interaction_start_time,Max(interaction_id) as Tiquete_ID,account_id
  From CallsWithoutSolution
  group by 1,3
  order by 3,1

)

,AllBillingCallsJoin as(
  Select Distinct a.interaction_start_time,a.account_id,a.interaction_id,b.Tiquete_ID as RepeatedIssue
  From BillingCalls a left join MultipleCallsFix b
  ON a.account_id=b.account_id and a.interaction_id=b.Tiquete_ID
  Where b.Tiquete_ID is null
)

,UniqueSuccesfulCalls as(
  Select Distinct interaction_start_time,account_id,Max(interaction_id) as Tiquete_ID
  From AllBillingCallsJoin
  group by 1,2

)

,SuccesfulCallsPerClient as(
  Select distinct date_trunc('MONTH',interaction_start_time) as TicketMonth,account_id,Count(distinct Tiquete_ID) as ResolvedBilling
  From UniqueSuccesfulCalls
  group by 1,2
)

,SuccessfulCalls_MasterTable as(
  Select f.*,ResolvedBilling as ResolvedBillingCalls
  From BillVariationCalls_MasterTable f left join SuccesfulCallsPerClient
  On Month=cast(TicketMonth as date) and Fixed_Account=account_id
)

,AllBillingRelatedCalls as(
  Select distinct date_trunc('MONTH',interaction_start_time) as CallMonth,account_id,count(distinct interaction_id) as AllBillingCalls
  From "db-stage-dev"."interactions_cabletica"
  WHERE interaction_purpose_descrip is not null and account_id is not null AND interaction_status <> 'ANULADA'
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
        'NOTAS DE CREDITO')
  group by 1,2
)

,AllBillingCalls_MasterTable as(
  Select f.*,AllBillingCalls as AllBillingCalls 
  From SuccessfulCalls_MasterTable f left join AllBillingRelatedCalls 
  ON account_id=Fixed_Account and Month=cast(CallMonth as date)

)
-------------------------------------------------------------- Grouped Table ------------------------------------------------------------------------------------------

Select Distinct Month,round(sum(B_NumRGUs),0) as FixedRGUs,
round(sum(TechCall_Flag),0) as TechCalls,
round(sum(TechCall_Flag)*1000/sum(B_NumRGUs),0) as TechCallsPer1kRGU,
round(sum(CareCall_Flag),0) as CareCalls,
round(sum(CareCall_Flag)*1000/sum(B_NumRGUs),0) as CareCallsPer1kRGU,
Count(distinct BillVariation_Flag) as BillVariations,
sum(BillingCalls) as BillingCalls,
round(sum(BillingCalls)/Count(distinct BillVariation_Flag),3) as BillingCallsPerBillVariation,
round(sum(ResolvedBillingCalls),0) as FTR_Billing,
sum(AllBillingCalls) as AllBillingCalls,
round(sum(ResolvedBillingCalls)/sum(AllBillingCalls),3) as FTR_Billing_KPI
From AllBillingCalls_MasterTable
where month<>date('2020-12-01') and Month <>date('2022-06-01')
group by 1
order by 1
