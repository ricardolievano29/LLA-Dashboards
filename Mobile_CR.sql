
-----------------------------------------------------------------------
                    -- VERSION FINAL MOBILE TABLE--
-----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS "lla_cco_int_san"."cr_mobile_table_dic_14" AS

WITH
-- select distinct fecha_parque From "cr_ext_parque_temp" 
MobileUsefulFields as(
Select distinct date_trunc('Month',date_add('Month',1,case when fecha_parque like '%/%' then cast(DATE_PARSE(CAST(fecha_parque AS VARCHAR(10)), '%d/%m/%Y') as date) else date(fecha_parque) end )) as Month, replace(ID_ABONADO,'.','') as ID_ABONADO,
case when contrato is not null and contrato<>'' and contrato<>'#N/D' THen contrato
else null end as FixedContract,Num_Telefono,Direccion_Correo,des_segmento_cliente,
case 
when (renta like '%#%' or renta like '%/%'or renta like 'NULL') then null -- Esta linea sirve para eliminar valores errones de renta 
else cast(replace(coalesce(renta,'0'),',','.') as double) end as renta

,CASE WHEN fch_activacion='#N/D' OR fch_activacion='SAMSUNG GALAXY A03 CORE 32GB NEGRO' OR fch_activacion='IPHONE 12 PRO MAX GRAFITO 256GB' 
OR fch_activacion='ARTICULO SIN EQUIPO' 

THEN NULL else (case when fecha_parque <> '30/11/2022' then date_parse(substring(fch_activacion,1,10),'%m/%d/%Y') else date_parse(substring(fch_activacion,1,10),'%d/%m/%Y') end) END as StartDate 

From "cr_ext_parque_temp"  --limit 10
WHERE DES_SEGMENTO_CLIENTE <>'Empresas - Empresas' AND DES_SEGMENTO_CLIENTE <>'Empresas - Pymes'

)

,CustomerBase_BOM as(
SELECT DISTINCT date_trunc('Month',Month) as B_Month,ID_ABONADO as B_mobile_account,FixedContract as b_fixed_contract,Renta as b_mobile_mrc,Num_Telefono as b_num_telefono,Direccion_correo b_correo, StartDate as b_start_date
From MobileUsefulFields
--where fixedcontract is not null
)

,CustomerBase_EOM as(
SELECT DISTINCT date_trunc('Month',date_add('Month',-1,Month)) as E_Month,ID_ABONADO as E_mobile_account,cast(FixedContract as varchar) as e_fixed_contract,Renta as e_mobile_mrc,Num_Telefono as e_num_telefono,Direccion_correo as e_correo, StartDate as e_start_date
From MobileUsefulFields
)

,MobileCustomerBase as(
SELECT DISTINCT
CASE WHEN (B_mobile_account IS NOT NULL AND E_mobile_account IS NOT NULL) OR (B_mobile_account IS NOT NULL AND 
E_mobile_account IS NULL) THEN B_Month
WHEN (B_mobile_account IS NULL AND E_mobile_account IS NOT NULL) THEN E_Month
END AS mobile_month,

CASE WHEN (B_mobile_account IS NOT NULL AND E_mobile_account IS NOT NULL) OR (B_mobile_account IS NOT NULL AND 
E_mobile_account IS NULL) THEN B_mobile_account
WHEN (B_mobile_account IS NULL AND E_mobile_account IS NOT NULL) THEN E_mobile_account
END AS mobile_account,



CASE WHEN B_mobile_account IS NOT NULL THEN 1 ELSE 0 END AS mobile_active_bom,
CASE WHEN E_mobile_account IS NOT NULL THEN 1 ELSE 0 END AS mobile_active_eom,

b_fixed_contract,b_num_telefono,b_correo,b_mobile_mrc,b_start_date,
e_fixed_contract,e_num_telefono,e_correo,e_mobile_mrc,e_start_date
FROM CustomerBase_BOM b FULL OUTER JOIN CustomerBase_EOM e ON B_mobile_account=E_mobile_account AND 
B_Month=E_Month
)


,FlagTenureCutomerBase as(
SELECT DISTINCT *, date_diff('Month',cast(b_start_date as date),cast(mobile_month as date)) AS b_mobile_tenure_months,
CASE WHEN date_diff('Month',cast(b_start_date as date),cast(mobile_month as date)) <6 THEN 'Early Tenure'
WHEN date_diff('Month',cast(b_start_date as date),cast(mobile_month as date)) >=6 THEN 'Late Tenure'
ELSE NULL END AS b_mobile_tenure_segment,
date_diff('Month',cast(e_start_date as date),cast(mobile_month as date)) AS e_mobile_tenure_months,
CASE WHEN date_diff('Month',cast(e_start_date as date),cast(mobile_month as date)) <6 THEN 'Early Tenure'
WHEN date_diff('Month',cast(e_start_date as date),cast(mobile_month as date)) >=6 THEN 'Late Tenure'
ELSE NULL END AS e_mobile_tenure_segment
From MobileCustomerBase
)


--------------------------------------- Main Movements ----------------------------------------------

,MainMovements as(
SELECT DISTINCT *, 

CASE 
WHEN b_fixed_contract is not null and b_fixed_contract<>'' and b_fixed_contract<>'#N/D' THEN cast(b_fixed_contract as varchar)
WHEN e_fixed_contract is not null and e_fixed_contract<>'' and e_fixed_contract<>'#N/D' THEN cast(e_fixed_contract as varchar)
WHEN mobile_account IS NOT NULL THEN cast(mobile_account as varchar)
END AS fmc_account,

CASE
WHEN mobile_active_bom =1 AND mobile_active_eom =1 AND(b_mobile_mrc=e_mobile_mrc) THEN '01.Maintain'
WHEN mobile_active_bom =1 AND mobile_active_eom =1 AND(b_mobile_mrc>e_mobile_mrc) THEN '02.Downspin'
WHEN mobile_active_bom =1 AND mobile_active_eom =1 AND(b_mobile_mrc<e_mobile_mrc) THEN '03.Upspin'
WHEN mobile_active_bom =1 AND mobile_active_eom =0 THEN '04.Loss'
--WHEN (mobile_active_bom=0 OR mobile_active_bom IS NULL)  AND mobile_active_eom=1 AND e_start_date <>mobile_month THEN '05.Come Back To Life'
--WHEN (mobile_active_bom=0 OR mobile_active_bom IS NULL)  AND mobile_active_eom=1 AND e_start_date =mobile_month THEN '06.New Customer'
WHEN (b_mobile_mrc IS NULL OR e_mobile_mrc IS NULL) THEN '07.MRC Gap'
ELSE NULL END AS mobile_movement_flag
From FlagTenureCutomerBase
)

--------------------------------------- Churners ---------------------------------------------------

,MobileChurners as(
SELECT *, '1. Mobile Churner' as MobileChurnFlag
FROM MainMovements
WHERE mobile_active_bom=1 AND mobile_active_eom=0
)
,Movements as(
Select *, CASE
WHEN TIPO_BAJA='ALTA/MIGRACION' THEN '2. Mobile Involuntary Churner'
WHEN TIPO_BAJA='BAJA INVOLUNTARIA' THEN '2. Mobile Involuntary Churner'
WHEN TIPO_BAJA='BAJA PORTABILIDAD' THEN '2. Mobile Involuntary Churner'
WHEN TIPO_BAJA='BAJA VOLUNTARIA' THEN '1. Mobile Voluntary Churner'
ELSE NULL END AS mobile_churn_type
From "cr_ext_mov_temp"
)

,ChurnersMovements as(
SELECT M.*,mobile_churn_type
FROM MobileChurners m LEFT JOIN Movements
ON mobile_account=cast(ID_Abonado as varchar) AND Date_trunc('Month',mobile_month)=Date_TRUNC('Month',date(dt))
)

,CustomerBaseWithChurn AS (
SELECT DISTINCT m.*,
case when mobile_churn_type is not null then mobile_churn_type
else '3. Mobile NonChurner' end as mobile_churn_flag, 
c.mobile_churn_type,case
when mobile_churn_type is not null then 1 else 0 end as mobile_rgu_churn
FROM MainMovements m LEFT JOIN ChurnersMovements c ON m.mobile_account=c.mobile_account 
and c.mobile_month=
m.mobile_month
)

------------------------------------------- Rejoiners ---------------------------------------------------

,inactive_users AS (
SELECT DISTINCT mobile_month AS exit_month, mobile_account as mobile_rejoiner,mobile_churn_type,case
when mobile_churn_type ='1. Mobile Voluntary Churner' then '1. Mobile Voluntary Rejoiner'
when mobile_churn_type ='2. Mobile Involuntary Churner' then '2. Mobile Involuntary Rejoiner'
else null end as mobile_rejoiner_type,
DATE_ADD('Month',1, mobile_month) AS rejoiner_month
FROM CustomerBaseWithChurn
WHERE mobile_churn_type is not null
)

,FullMobileBase_Rejoiners AS(
SELECT DISTINCT f.*,mobile_rejoiner_type
FROM CustomerBaseWithChurn f LEFT JOIN inactive_users r ON f.mobile_account=r.mobile_rejoiner AND f.mobile_month=rejoiner_month
)

select * From FullMobileBase_Rejoiners
--limit 10
