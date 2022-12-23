-----------------------------------------------------------------------
                    -- VERSION FINAL FMC TABLE--
-----------------------------------------------------------------------

--CREATE TABLE IF NOT EXISTS "lla_cco_int_san"."cr_fmc_table"  AS  

WITH 


Fixed_Base AS(
  SELECT DISTINCT * FROM "lla_cco_int_san"."cr_fixed_table"

)

,Mobile_Base AS(
  SELECT DISTINCT * FROM "lla_cco_int_san"."cr_mobile_table" --limit 10

)

---------------------------------------------- Near FMC ---------------------------------

,Near_BOM as(
SELECT DISTINCT FECHA_PARQUE,replace(replace(ID_ABONADO,'.',''),',','') as ID_ABONADO, act_acct_cd, NOM_EMAIL AS b_email
FROM "cr_ext_parque_temp" inner join "db-analytics-dev"."dna_fixed_cr" 
ON NOM_EMAIL=ACT_CONTACT_MAIL_1
WHERE DES_SEGMENTO_CLIENTE <>'Empresas - Empresas' AND DES_SEGMENTO_CLIENTE <>'Empresas - Pymes' 
AND NOM_EMAIL <>'NOTIENE@GMAIL.COM' AND NOM_EMAIL<> 'NorEPorTA@CABLETICA.COM' AND NOM_EMAIL<>'NorEPorTACorREO@CABLETICA.COM'
AND NOM_EMAIL<>'NorEPorTA.@CABLETICA.COM' AND NOM_EMAIL<>'NOTIENE@CABLETICA.COM' AND NOM_EMAIL<>'NA@GMAIL.COM'
AND NOM_EMAIL<>'NOTIENE@NOTIENE.COM' AND NOM_EMAIL<>'NorEPorTA@LIBERTY.COM' 
AND NOM_EMAIL<>'NO@GMAIL.COM'
)

,NEARFMC_MOBILE_BOM AS (
SELECT DISTINCT a.*,b.act_acct_cd AS b_contr, b.b_email
FROM Mobile_Base a LEFT JOIN Near_BOM b 
ON ID_ABONADO=Mobile_Account AND cast(FECHA_PARQUE as varchar)=cast(Mobile_month as varchar)
)


,EMAIL_EOM AS (
SELECT DISTINCT FECHA_PARQUE,replace(replace(ID_ABONADO,'.',''),',','') as ID_ABONADO, act_acct_cd, NOM_EMAIL AS e_email
FROM "dna_mobile_historic_cr" inner join "db-analytics-dev"."dna_fixed_cr" 
ON NOM_EMAIL=ACT_CONTACT_MAIL_1 
WHERE DES_SEGMENTO_CLIENTE <>'Empresas - Empresas' AND DES_SEGMENTO_CLIENTE <>'Empresas - Pymes' 
AND NOM_EMAIL <>'NOTIENE@GMAIL.COM' AND NOM_EMAIL<> 'NorEPorTA@CABLETICA.COM' AND NOM_EMAIL<>'NorEPorTACorREO@CABLETICA.COM'
)

,NEARFMC_MOBILE_EOM AS (
SELECT DISTINCT a.*,b.act_acct_cd AS e_contr, e_email
FROM NEARFMC_MOBILE_BOM a LEFT JOIN EMAIL_EOM b 
ON ID_ABONADO=Mobile_Account AND DATE_ADD('month',-1,cast(Fecha_Parque as date) )=cast(Mobile_month as date)
)

,FullCustomerBase as(
SELECT DISTINCT
CASE WHEN (Fixed_Account IS NOT NULL AND Mobile_Account IS NOT NULL) or (Fixed_Account IS NOT NULL AND Mobile_Account IS NULL) THEN Fixed_month
      WHEN (Fixed_Account IS NULL AND Mobile_Account IS NOT NULL) THEN Mobile_month
  END AS month,
CASE 
WHEN (Fixed_Account IS NOT NULL AND Mobile_Account IS NULL) THEN Fixed_Account
WHEN (Fixed_Account IS NULL AND Mobile_Account IS NOT NULL) THEN Mobile_Account
WHEN (Fixed_Account IS NOT NULL AND Mobile_Account IS NOT NULL) THEN Concat(Fixed_Account,Mobile_Account)
END AS final_account,
CASE 
WHEN (active_bom =1 AND Mobile_active_bom=1) or (active_bom=1 AND (Mobile_active_bom=0 or Mobile_active_bom IS NULL)) or ((active_bom=0 or active_bom IS NULL) AND Mobile_active_bom=1) THEN 1 ELSE 0 END AS final_bom_active_flag,
CASE 
WHEN (active_eom =1 AND Mobile_active_eom=1) or (active_eom=1 AND (Mobile_active_eom=0 or Mobile_active_eom IS NULL)) or ((active_eom=0 or active_eom IS NULL) AND Mobile_active_eom=1) THEN 1
ELSE 0 END AS final_eom_active_flag,
CASE
WHEN (Fixed_Account is not null and Mobile_Account is not null and active_bom = 1 and Mobile_active_bom = 1 AND b_fixed_contract IS NOT NULL ) THEN 'Soft FMC'
WHEN (b_email IS NOT NULL AND b_fixed_contract IS NULL AND active_bom=1) or (active_bom = 1 and Mobile_active_bom = 1)  THEN 'Near FMC'
WHEN (Fixed_Account IS NOT NULL AND active_bom=1 AND (Mobile_active_bom = 0 or Mobile_active_bom IS NULL))  THEN 'Fixed Only'
WHEN ((Mobile_Account IS NOT NULL AND Mobile_active_bom=1 AND (active_bom = 0 or active_bom IS NULL)))  THEN 'Mobile Only'
END AS b_fmc_status,
CASE 
WHEN (Fixed_Account is not null and Mobile_Account is not null and active_eom = 1 and Mobile_active_eom = 1 AND e_fixed_contract IS NOT NULL ) THEN 'Soft FMC'
WHEN (e_email IS NOT NULL AND e_fixed_contract IS NULL AND active_eom=1) or (active_eom = 1 and Mobile_active_eom = 1) THEN 'Near FMC'
WHEN (Fixed_Account IS NOT NULL AND active_eom=1 AND (Mobile_active_eom = 0 or Mobile_active_eom IS NULL))  THEN 'Fixed Only'
WHEN (Mobile_Account IS NOT NULL AND Mobile_active_eom=1 AND (active_eom=0 or active_eom IS NULL )) AND (mobile_churn_type<>'1. Mobile Churner' or mobile_churn_type is null) THEN 'Mobile Only'
END AS e_fmc_status,f.*,m.*
FROM Fixed_Base f FULL OUTER JOIN NEARFMC_MOBILE_EOM  m 
ON reverse(rpad(substr(reverse(fixed_account),1,10),10,'0'))=cast(reverse(rpad(substr(reverse(fmc_account),1,10),10,'0')) as varchar) AND Fixed_month=Mobile_month
)

,repeated_fix as(
select distinct month,fixed_account,count(*) as fixed_count From FullCustomerBase
group by 1,2
)

,repeated_fix_master_table as(
select a.*,case 
when fixed_count is null then 1 else fixed_count end as  contracts_fix
from FullCustomerBase a left join repeated_fix b
on a.month=b.month and a.fixed_account=b.fixed_account
)



,CustomerBase_FMC_Tech_Flags AS(
 SELECT t.*,
coalesce(cast(B_BILL_AMT as integer),0)/contracts_fix + coalesce(cast(B_Mobile_MRC as integer),0) AS b_total_mrc ,  (coalesce(cast(E_BILL_AMT as integer),0)/contracts_fix + coalesce(cast(E_Mobile_MRC as integer),0)) AS e_total_mrc,

CASE  
WHEN (b_fmc_status = 'Fixed Only' or b_fmc_status = 'Soft FMC' or b_fmc_status='Near FMC' )  AND (Mobile_active_bom = 0 or MOBILE_active_bom IS NULL) AND B_MIX = '1P' THEN 'Fixed 1P'
WHEN (b_fmc_status = 'Fixed Only' or b_fmc_status = 'Soft FMC' or b_fmc_status='Near FMC' )  AND (Mobile_active_bom = 0 or MOBILE_active_bom IS NULL) AND B_MIX = '2P' THEN 'Fixed 2P'
WHEN (b_fmc_status = 'Fixed Only' or b_fmc_status = 'Soft FMC' or b_fmc_status='Near FMC' )  AND (Mobile_active_bom = 0 or MOBILE_active_bom IS NULL) AND B_MIX = '3P' THEN 'Fixed 3P'
WHEN (b_fmc_status = 'Soft FMC' or b_fmc_status='Near FMC') AND (active_bom = 0 or active_bom is null) then 'Mobile Only'
WHEN b_fmc_status = 'Mobile Only' THEN b_fmc_status
WHEN (b_fmc_status='Near FMC' or  b_fmc_status='Soft FMC') THEN b_fmc_status
WHEN final_bom_active_flag=1 and b_num_rgus=0 THEN 'Fixed 0P'
END AS b_fmc_type,

CASE 
--WHEN final_eom_active_flag = 0 AND ((active_eom = 0 AND fixed_churner_type IS NULL) or (Mobile_active_eom = 0 AND mobile_churn_type is null)) THEN "Customer Gap"
WHEN e_fmc_status = 'Fixed Only' AND fixed_churner_type IS NOT NULL THEN NULL
WHEN e_fmc_status = 'Mobile Only' AND mobile_churn_type ='1. Mobile Churner' THEN NULL
WHEN (e_fmc_status = 'Fixed Only')  AND (Mobile_active_eom = 0 or MOBILE_active_eom IS NULL or(Mobile_active_eom = 1 AND mobile_churn_type IS NOT NULL))  AND E_MIX = '1P' THEN 'Fixed 1P'
WHEN (e_fmc_status = 'Fixed Only' )  AND (Mobile_active_eom = 0 or MOBILE_active_eom IS NULL or(Mobile_active_eom = 1 AND mobile_churn_type IS NOT NULL)) AND E_MIX = '2P' THEN 'Fixed 2P'
WHEN (e_fmc_status = 'Fixed Only' )  AND (Mobile_active_eom = 0 or MOBILE_active_eom IS NULL or(Mobile_active_eom = 1 AND mobile_churn_type IS NOT NULL)) AND E_MIX = '3P' THEN 'Fixed 3P'
WHEN (e_fmc_status = 'Soft FMC' or e_fmc_status = 'Near FMC' or e_fmc_status='Mobile Only') AND (active_eom = 0 or active_eom is null or (active_eom = 1 AND fixed_churner_type IS NOT NULL)) or (e_fmc_status = 'Mobile Only' or((active_eom is null or active_eom=0) and(Mobile_active_eom=1))) THEN 'Mobile Only'
WHEN (e_fmc_status='Soft FMC' or e_fmc_status='Near FMC') AND (fixed_churner_type IS NULL AND mobile_churn_type<>'1. Mobile Churner' AND active_eom=1 AND Mobile_active_eom=1 ) THEN e_fmc_status
WHEN final_eom_active_flag=1 and e_num_rgus=0 THEN 'Fixed 0P'
END AS e_fmc_type
,case when Mobile_active_bom=1 then 1 else 0 end as b_mobile_rgus
,case when Mobile_active_eom=1 then 1 else 0 end as e_mobile_rgus
 FROM repeated_fix_master_table t
)
 
,CustomerBase_FMCSegments_ChurnFlag AS(
SELECT c.*,
CASE WHEN (b_fmc_status = 'Fixed Only') or ((b_fmc_status = 'Soft FMC' or b_fmc_status='Near FMC') AND active_bom = 1 AND Mobile_active_bom = 1) THEN b_tech_flag
WHEN b_fmc_status = 'Mobile Only' or ((b_fmc_status = 'Soft FMC' or b_fmc_status='Near FMC' or b_fmc_status='Undefined FMC') AND (active_bom = 0 or active_bom IS NULL)) THEN 'Wireless'
END AS b_final_tech_flag,
CASE
WHEN (e_fmc_status = 'Fixed Only' AND fixed_churner_type is null) or ((e_fmc_status = 'Soft FMC' or e_fmc_status='Near FMC') AND active_eom = 1 AND Mobile_active_eom = 1 AND fixed_churner_type is null) THEN e_tech_flag
WHEN e_fmc_status = 'Mobile Only' or ((e_fmc_status = 'Soft FMC' or e_fmc_status='Near FMC') AND (active_eom = 0 or active_eom IS NULL)) THEN 'Wireless'
END AS e_final_tech_flag,
case 
when b_fixed_tenure_segment='Late Tenure' or b_fixed_tenure_segment='Early Tenure' or b_fixed_tenure_segment='Mid Tenure' then b_fixed_tenure_segment
when b_mobile_tenure_segment='Late Tenure' or b_mobile_tenure_segment='Early Tenure' or b_mobile_tenure_segment='Mid Tenure' then b_mobile_tenure_segment
else null end as b_tenure_final_flag,

case 
when (e_fixed_tenure_segment='Late Tenure' or e_fixed_tenure_segment='Early Tenure' or e_fixed_tenure_segment='Mid Tenure') 
and fixed_churner_type is null then e_fixed_tenure_segment
when (e_mobile_tenure_segment='Late Tenure' or e_mobile_tenure_segment='Early Tenure' or e_mobile_tenure_segment='Mid Tenure')
and mobile_churn_type is null then e_mobile_tenure_segment
else null end as e_tenure_final_flag,


/*
CASE WHEN (b_fixed_tenure_segment =  'Late Tenure' and b_mobile_tenure_segment =  'Late Tenure') or (b_fixed_tenure_segment =  'Late Tenure' and b_mobile_tenure_segment is null) or (b_fixed_tenure_segment IS NULL and b_mobile_tenure_segment =  'Late Tenure') THEN 'Late Tenure'
 WHEN (b_fixed_tenure_segment =  'Early Tenure' or b_mobile_tenure_segment =  'Early Tenure') THEN 'Early Tenure'
 END AS b_tenure_final_flag,
  CASE WHEN (e_fixed_tenure_segment =  'Late Tenure' and e_mobile_tenure_segment =  'Late Tenure') or (e_fixed_tenure_segment =  'Late Tenure' and e_mobile_tenure_segment is null) or (e_fixed_tenure_segment IS NULL and e_mobile_tenure_segment =  'Late Tenure') THEN 'Late Tenure'
 WHEN (e_fixed_tenure_segment =  'Early Tenure' or e_mobile_tenure_segment =  'Early Tenure') THEN 'Early Tenure'
 END AS e_tenure_final_flag,
*/
CASE
WHEN (b_fmc_type = 'Soft FMC' or b_fmc_type = 'Near FMC') AND B_MIX = '1P'  THEN 'P2'
WHEN (b_fmc_type  = 'Soft FMC' or b_fmc_type = 'Near FMC') AND B_MIX = '2P' THEN 'P3'
WHEN (b_fmc_type  = 'Soft FMC' or b_fmc_type = 'Near FMC') AND B_MIX = '3P' THEN 'P4'

WHEN (b_fmc_type  = 'Soft FMC' or b_fmc_type = 'Near FMC') AND B_MIX = '0P' THEN 'P0'

WHEN (b_fmc_type  = 'Fixed 1P' or b_fmc_type  = 'Fixed 2P' or b_fmc_type  = 'Fixed 3P') or ((b_fmc_type  = 'Soft FMC' or b_fmc_type='Near FMC') AND(Mobile_active_bom= 0 or Mobile_active_bom IS NULL)) AND active_bom = 1 THEN 'P1_Fixed'
WHEN (b_fmc_type = 'Mobile Only')  or (b_fmc_type  = 'Soft FMC' AND(active_bom= 0 or active_bom IS NULL)) AND Mobile_active_bom = 1 THEN 'P1_Mobile'
WHEN (b_fmc_type  = 'Fixed 0P') or ((b_fmc_type  = 'Soft FMC' or b_fmc_type='Near FMC') AND(Mobile_active_bom= 0 or Mobile_active_bom IS NULL)) AND active_bom = 1 THEN 'P0_Fixed'

END AS b_fmc_segment,
CASE 
--WHEN e_fmc_type="Customer Gap" THEN "Customer Gap" 
WHEN (e_fmc_type = 'Soft FMC' or e_fmc_type='Near FMC') AND (active_eom = 1 and Mobile_active_eom=1) AND E_MIX = '1P' AND (fixed_churner_type IS NULL and mobile_churn_type IS NULL) THEN 'P2'
WHEN (e_fmc_type  = 'Soft FMC' or e_fmc_type='Near FMC' or e_fmc_type='Undefined FMC') AND (active_eom = 1 and Mobile_active_eom=1) AND E_MIX = '2P' AND (fixed_churner_type IS NULL and mobile_churn_type IS NULL) THEN 'P3'
WHEN (e_fmc_type  = 'Soft FMC' or e_fmc_type='Near FMC' or e_fmc_type='Undefined FMC') AND (active_eom = 1 and Mobile_active_eom=1) AND E_MIX = '3P' AND (fixed_churner_type IS NULL and mobile_churn_type IS NULL) THEN 'P4'
WHEN ((e_fmc_type  = 'Fixed 1P' or e_fmc_type  = 'Fixed 2P' or e_fmc_type  = 'Fixed 3P') or ((e_fmc_type  = 'Soft FMC' or e_fmc_type='Near FMC') AND(Mobile_active_eom= 0 or Mobile_active_eom IS NULL))) AND (active_eom = 1 AND fixed_churner_type IS NULL) THEN 'P1_Fixed'
WHEN ((e_fmc_type = 'Mobile Only')  or (e_fmc_type  ='Soft FMC' AND(active_eom= 0 or active_eom IS NULL))) AND (Mobile_active_eom = 1 and mobile_churn_type IS NULL) THEN 'P1_Mobile'

WHEN (e_fmc_type  = 'Fixed 0P') or ((e_fmc_type  = 'Soft FMC' or e_fmc_type='Near FMC') AND(Mobile_active_eom= 0 or Mobile_active_eom IS NULL)) AND active_eom = 1 THEN 'P0_Fixed'

WHEN (e_fmc_type  = 'Soft FMC' or e_fmc_type = 'Near FMC') AND E_MIX = '0P' THEN 'P0'

END AS e_fmc_segment,case
when (fixed_churner_type is not null and mobile_churn_type is not null) or (b_fmc_status = 'Fixed Only' and fixed_churner_type is not null) 
or (b_fmc_status = 'Mobile Only' and mobile_churn_type is not null) or (fixed_churner_type is null and active_bom=1 and mobile_active_bom=1 AND ((active_eom=0 or active_eom is null) and (Mobile_active_eom=0 or mobile_active_eom Is null))) THEN 'Full Churner'
when (fixed_churner_type is not null and mobile_churn_type is null) then 'Fixed Churner'
when (fixed_churner_type is null and mobile_churn_type is NOT null) then 'Mobile Churner'
when (fixed_churner_type is not null  AND (active_bom IS NULL or active_bom = 0)) or (mobile_churn_type is not null and (Mobile_active_bom = 0 or Mobile_active_bom IS NULL)) THEN 'Previous churner' -- arreglar los previous churner de una mejor manera
ELSE 'Non Churner' END AS final_churn_flag

,(coalesce(b_num_rgus,0) + coalesce(b_mobile_rgus,0))/contracts_fix as b_total_rgus
,(coalesce(e_num_rgus,0) + coalesce(e_mobile_rgus,0))/contracts_fix AS e_total_rgus
,round(e_total_mrc,0) - round(b_total_mrc,0) AS mrc_change
,(coalesce(fixed_rgu_churn,0)/contracts_fix + coalesce(mobile_rgu_churn,0)) as total_rgu_churn
FROM CustomerBase_FMC_Tech_Flags c
)

,RejoinerColumn AS (
select distinct  f.*,case
when (fixed_rejoiner_type is not null and mobile_rejoiner_type is not null) or 
((fixed_rejoiner_type is not null or mobile_rejoiner_type is not null) and (e_fmc_type = 'Soft FMC' or e_fmc_type = 'Near FMC')) then 'FMC Rejoiner'
when fixed_rejoiner_type is not null then 'Fixed Rejoiner'
when mobile_rejoiner_type is not null then 'Mobile Rejoiner'
end as rejoiner_final_flag
FROM CustomerBase_FMCSegments_ChurnFlag f
)

-------------------------------------- Waterfall -------------------------------------

,FullCustomersBase_Flags_Waterfall AS(
SELECT DISTINCT f.*,
CASE 
WHEN final_churn_flag ='Full Churner' THEN 'Total Churner'
WHEN final_churn_flag='Fixed Churner' or final_churn_flag='Mobile Churner' THEN 'Partial Churner'
WHEN final_churn_flag = 'Non Churner' then null
WHEN final_churn_flag = 'Previous churner' then 'Previous churner'
ELSE null end as partial_total_churn_flag,
case
when fixed_churner_type='1. Fixed Voluntary Churner' Then 'Voluntary'
when mobile_churn_type='1. Mobile Voluntary Churner'   Then 'Voluntary'
when fixed_churner_type='2. Fixed Involuntary Churner' Then 'Involuntary'
when mobile_churn_type='2. Mobile Involuntary Churner' Then 'Involuntary'
when main_movement='03. Downsell' then 'Voluntary'
End as churn_type_final_flag
,case
when final_churn_flag<>'Non Churner' then final_churn_flag
when b_total_rgus=e_total_rgus and b_total_mrc=e_total_mrc then 'Maintain'
when b_total_rgus<e_total_rgus then 'Upsell'
when b_total_rgus>e_total_rgus then 'Downsell'
when b_total_rgus=e_total_rgus and b_total_mrc<e_total_mrc then 'Upspin'
when b_total_rgus=e_total_rgus and b_total_mrc>e_total_mrc then 'Downspin'
when (b_fmc_type='Fixed 1P' or b_fmc_type='Fixed 2P' or b_fmc_type='Fixed 3P' or b_fmc_type= 'Fixed 0P' or b_fmc_type='Mobile Only') and
(e_fmc_type = 'Soft FMC' or e_fmc_type='Near FMC') then 'FMC Packing'
when final_bom_active_flag=0 and (e_fmc_type = 'Soft FMC' or e_fmc_type='Near FMC') then 'FMC Gross Add'
when fixed_rejoiner_type is not null and mobile_rejoiner_type is not null then 'FMC Rejoiner'
when fixed_rejoiner_type is not null then 'Fixed Rejoiner'
when mobile_rejoiner_type is not null then 'Mobile Rejoiner'
when (main_movement='04. Come Back to Life' or main_movement='05. New Customer') and (mobile_movement_flag='05.Come Back To Life' or mobile_movement_flag='06.New Customer')
then 'FMC Gross Add'
when (main_movement='04. Come Back to Life' or main_movement='05. New Customer') then 'Fixed Gross Add'
when (mobile_movement_flag='05.Come Back To Life' or mobile_movement_flag='06.New Customer') then 'Mobile Gross Add'
END AS waterfall_flag,
CONCAT(coalesce(B_Plan,''),cast(coalesce(cast(Mobile_active_bom as varchar),'-') as varchar),'') AS b_plan_full, 
CONCAT(coalesce(E_Plan,''),cast(coalesce(cast(Mobile_active_eom as varchar),'-') as varchar),'') AS e_plan_full 


FROM RejoinerColumn f
)


,Last_Flags as(
select *
,Case when waterfall_flag='Downsell' and (main_movement='03. Downsell' ) then 'Voluntary'
      when waterfall_flag='Downsell' and final_churn_flag <> 'Non Churner' then churn_type_final_flag
      when waterfall_flag='Downsell' and main_movement='Loss' then 'Undefined'
else null end as downsell_split
,case when waterfall_flag='Downspin' then 'Voluntary' else null end as downspin_split
from FullCustomersBase_Flags_Waterfall
)

select * from last_flags
