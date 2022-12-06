WITH 

FMC_Table AS(
  SELECT *,
  'CT' AS opco,'Costa_Rica' AS market,'large' AS marketSize,'Fixed' AS product,'B2C' AS biz_unit,
  Case when MainMovement='New Customer' THEN Fixed_Account Else null end as Gross_Adds,
  Case when Fixed_account is not null then Fixed_Account Else null end as Active_Base
   FROM "lla_cco_int_san"."cr_fmc_table"


)

Sprint3_KPIs as(-- falta arreglar soft dx y toca hacer mounting bills
  select distinct Month,sum(activebase) as activebase,sum(sales) as unique_sales,sum(MountingBills) as unique_mountingbills,
  sum(Long_Installs) as unique_longinstalls,sum(EarlyIssueCall) as unique_earlyinteraction,sum(TechCalls) as unique_earlyticket,
  sum(BillClaim) as unique_billclaim,sum(MRC_Change) as unique_mrcchange,sum(NoPlan_Changes) as noplan
  From "lla_cco_int_san"."cr_Sprint 3"
  Where Month<>date('2020-12-01') and Month<>date('2022-06-01')
  group by 1
)

,S3_CX_KPIs as(
  select distinct Month,'CT' AS opco,'Costa_Rica' AS market,'large' AS MarketSize,'Fixed' AS product,'B2C' AS biz_unit,
  activebase,unique_mrcchange as mrc_change,noplan as noplan_customers,unique_sales,unique_longinstalls,
  unique_earlyticket,unique_earlyinteraction,
  round(unique_mrcchange/noplan,4) as Customers_w_MRC_Changes,round(sum(unique_mountingbills),0) as MountingBills,
  round(unique_longinstalls/unique_sales,4) as breech_cases_installs,round(unique_earlyticket/unique_sales,4) as Early_Tech_Tix,
  round(unique_earlyinteraction/unique_sales,4) as New_Customer_Callers
  From Sprint3_KPIs 
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)

,Sprint3_Sales_KPIs as(
  select distinct 
  -- Ajuste pendiente cuando se arregle el problema de la columna de sales_month en el DNA 
  -- Sales_Month as Month, 
  sum(sales) as unique_sales,sum(Long_Installs) as unique_longinstalls,
  sum(EarlyIssueCall) as unique_earlyinteraction,sum(TechCalls) as unique_earlyticket,sum(Soft_Dx) as unique_softdx
  From "lla_cco_int_san"."cr_Sprint 3"
  
  -- Ajuste pendiente cuando se arregle el problema de la columna de sales_month en el DNA
  -- Where Sales_Month>='2021-01-01'
  --group by 1
)

,S3_Sales_CX_KPIs as(
  select distinct
  -- Ajuste pendiente cuando se arregle el problema de la columna de sales_month en el DNA
  --cast(Month as VARCHAR) as Month,
  'CT' AS opco,'Costa_Rica' AS market,'large' AS MarketSize,'Fixed' AS product,'B2C' AS biz_unit,
  unique_sales,unique_longinstalls,unique_earlyticket,unique_earlyinteraction,unique_softdx,
  round(unique_longinstalls/unique_sales,4) as breech_cases_installs,round(unique_earlyticket/unique_sales,4) as Early_Tech_Tix,
  round(unique_earlyinteraction/unique_sales,4) as New_Customer_Callers,round(unique_softdx/unique_sales,4) as New_Sales_to_Soft_Dx
  From Sprint3_Sales_KPIs 
)

,Sprint5_KPIs as(
  select Month,sum(activebase) as activebase, sum(TwoCalls_Flag)+sum(MultipleCalls_Flag) as RepeatedCallers,
  sum(TicketDensity_Flag) as numbertickets
  From "lla_cco_int_san"."cr_Sprint 5"
  group by 1
)

,S5_CX_KPIs as(
  select distinct Month,"CT" AS opco,"Costa_Rica" AS market,"large" AS MarketSize,"Fixed" AS product,"B2C" AS biz_unit,
  sum(activebase) as fixed_acc,sum(repeatedcallers) as repeat_callers,sum(numbertickets) as tickets,
  sum(RepeatedCallers/activebase) as Repeated_Callers,sum(numbertickets)/sum(activebase) as Tech_Tix_per_100_Acct
  From Sprint5_KPIs
  group by 1
)

,Additional_KPIs as(
  Select Distinct Month,sum(FixedRGUs) as FixedRGUs,sum(TechCalls) as TechCalls,sum(CareCalls) as CareCalls,sum(BillVariations) as BillVariations
  ,sum(BillingCalls) as BillingCalls,sum(AllBillingCalls) as AllBillingCalls,sum(FTR_Billing) as FTR_Billing
  From `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Additional_Cx_Table_DashboardInput_v2`
  group by 1
)

,Additional_CX_KPIs as(
  Select distinct Month,"CT" AS opco,"Costa_Rica" AS market,"large" AS MarketSize,"Fixed" AS product,"B2C" AS biz_unit,
  sum(FixedRGUs) as unique_FixedRGUs,sum(TechCalls) as unique_TechCalls,sum(CareCalls) as unique_CareCalls,
  sum(BillVariations) as unique_BillVariations,sum(BillingCalls) as unique_BillingCallsBillVariations,
  sum(AllBillingCalls) as unique_allbillingcalls,sum(FTR_Billing) as unique_FTR_Billing
  From Additional_KPIs
  group by 1,2,3,4,5
)


############################################################################### New KPIs ##################################################################################

/*
--,payments as(
  select distinct month,opco,market,marketsize,product,biz_unit,'pay' as journey_waypoint,'digital_shift' as facet,'%digital_payments' as kpi_name,
  round(sum(digital)/sum(pymt_cnt)*100,2) as kpi_meas
from( select date_trunc(clearing_date,Month) as Month,"CT" AS opco,"Costa_Rica" AS market,"large" AS marketSize,"Fixed" AS product,"B2C" AS biz_unit,
count(distinct(payment_doc_id)) as pymt_cnt,
case when digital_nondigital = 'Digital' then count(distinct(payment_doc_id)) end as digital
From `dev-fortress-335113.cabletica_ontological_prod_final.payment` 
group by 1,2,3,4,5,6,7
)
*/

,service_delivery as(
  Select Distinct safe_cast(Month as string) as Month,'CT' as Opco,'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,sum(Installations) as Install,
  round(sum(Inst_MTTI)/sum(Installations),2) as MTTI,sum(Repairs) as Repairs,round(sum(Rep_MTTR)/sum(Repairs),2) as MTTR,round(sum(scr),2) as Repairs_1k_rgu,
  round((sum(FTR_Install_M)/sum(Installations))/100,4) as FTR_Install,round((sum(FTR_Repair_M)/sum(Repairs))/100,4) as FTR_Repair

From(
  Select Distinct Date_Trunc(End_Week_Date,Month) as Month,Network,End_Week_Date,sum(Total_Subscribers) as Total_Users,sum(Assisted_Installations) as Installations,sum(mtti) as MTTI, 
  sum(Assisted_Installations)*sum(mtti) as Inst_MTTI,sum(truck_rolls) as Repairs,sum(mttr) as MTTR,sum(truck_rolls)*sum(mttr) as Rep_MTTR,sum(scr) as SCR,(100-sum(i_elf_28days)) as
  FTR_Install,(100-sum(r_elf_28days)) as FTR_Repair,(100-sum(i_elf_28days))*sum(Assisted_Installations) as FTR_Install_M,(100-sum(r_elf_28days))*sum(truck_rolls) as FTR_Repair_M
  from `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220725_Service_Delivery_KPIResults`
  where market='Costa Rica' and network='OVERALL'
  group by 1,2,3
  order by 1,2,3) group by 1,2,3,4,5,6 order by 1,2,3,4,5,6
)







########################################################################### All Flags KPIs ################################################################################
--Prev Calculated
,GrossAdds_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'buy' as journey_waypoint,'Gross_Adds' as kpi_name,
  count(distinct Gross_Adds) as kpi_meas,0 as kpi_num,0 as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network from FMC_Table group by 1,2,3,4,5,6,7,8,9
)

,ActiveBase_Flag1 as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'use' as journey_waypoint,'Active_Base' as kpi_name,
  count(distinct Active_Base) as kpi_meas,0 as kpi_num,0 as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network from FMC_Table group by 1,2,3,4,5,6,7,8,9
)

,ActiveBase_Flag2 as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-call' as journey_waypoint,'Active_Base' as kpi_name,
  count(distinct Active_Base) as kpi_meas,0 as kpi_num,0 as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network from FMC_Table group by 1,2,3,4,5,6,7,8,9
)

,TechTickets_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'use' as journey_waypoint,'Tech_Tix_per_100_Acct' as kpi_name,
  round(Tech_Tix_per_100_Acct,4) as kpi_meas,tickets as kpi_num,fixed_acc as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network from S5_CX_KPIs
)

,MRCChanges_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'pay' as journey_waypoint,'%Customers_w_MRC_Changes_5%+_excl_plan' as kpi_name,
round(Customers_w_MRC_Changes,4) as kpi_meas,mrc_change as kpi_num,noplan_customers as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network From S3_CX_KPIs
)

,SalesSoftDx_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'%New_Sales_to_Soft_Dx' as kpi_name,
  round(New_Sales_to_Soft_Dx,4) as kpi_meas,unique_softdx as kpi_num,unique_sales as kpi_den,null as KPI_Sla,'M-1' as Kpi_delay_display,null as Network From S3_Sales_CX_KPIs
)

,EarlyIssues_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'%New_Customer_Callers_2+calls_21days' as kpi_name,
  round(New_Customer_Callers,4) as kpi_meas,unique_earlyinteraction as kpi_num,unique_sales as kpi_den,null as KPI_Sla,'M-1' as Kpi_delay_display,null as Network From S3_Sales_CX_KPIs
)

,LongInstall_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'%breech_cases_install_6+days' as kpi_name,
  round(breech_cases_installs,4) as kpi_meas,unique_longinstalls as kpi_num,unique_sales as kpi_den,null as KPI_Sla,'M-1' as Kpi_delay_display,null as Network From S3_Sales_CX_KPIs
)

,EarlyTickets_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'%Early_Tech_Tix_-7weeks' as kpi_name,
  round(early_tech_tix,4) as kpi_meas,unique_earlyticket as kpi_num,unique_sales as kpi_den,null as KPI_Sla,'M-2' as Kpi_delay_display,null as Network From S3_Sales_CX_KPIs
)

,RepeatedCall_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-call' as journey_waypoint,'%Repeat_Callers_2+calls' as kpi_name,
  round(Repeated_Callers,4) as kpi_meas,repeat_callers as kpi_num,fixed_acc as kpi_den,null as KPI_Sla,'M-2' as Kpi_delay_display,null as Network From S5_CX_KPIs
)

,TechCall1kRGU_Flag as(
  Select Distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,'tech_calls_per_1k_rgu' as kpi_name,
  round(sum(unique_TechCalls)*1000/sum(unique_FixedRGUs),0) as kpi_meas,unique_TechCalls as kpi_num,unique_FixedRGUs as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,
  null as Network From
  Additional_CX_KPIs
  group by 1,2,3,4,5,6,7,8,11,12
)

,CareCall1kRGU_Flag as(
  Select Distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,'care_calls_per_1k_rgu' as kpi_name,
  round(sum(unique_CareCalls)*1000/sum(unique_FixedRGUs),0) as kpi_meas,unique_CareCalls as kpi_num,unique_FixedRGUs as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,
  null as Network From Additional_CX_KPIs
  group by 1,2,3,4,5,6,7,8,11,12
)

,BillingCallsPerBillVariation_Flag as(
  Select Distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'pay' as journey_waypoint,'Billing Calls per Bill Variation' as kpi_name,
  round(sum(unique_BillingCallsBillVariations)/sum(unique_BillVariations),3) as kpi_meas,unique_BillingCallsBillVariations as kpi_num,unique_BillVariations as kpi_den,
  null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network
  From Additional_CX_KPIs
  group by 1,2,3,4,5,6,7,8,11,12
)

,FTRBilling_Flag as(
  Select Distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'pay' as journey_waypoint,'%FTR_Billing' as kpi_name,
  sum(unique_FTR_Billing)/sum(unique_allbillingcalls) as kpi_meas,unique_FTR_Billing as kpi_num,unique_allbillingcalls as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network 
  From Additional_CX_KPIs
  group by 1,2,3,4,5,6,7,8,11,12
)

,MountingBill_Flag as(
select distinct  month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'high_risk' as facet,'pay' as journey_waypoint,'%Customers_w_Mounting_Bills' as kpi_name,MountingBills as kpi_meas,null as kpi_num,null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network from S3_CX_KPIs)


,installs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'get' as journey_waypoint,'Installs' as kpi_name, Install as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network from service_delivery)
,MTTI as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'get' as journey_waypoint,'MTTI' as kpi_name, mtti as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network from service_delivery)
,ftr_installs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'get' as journey_waypoint,'%FTR_installs' as kpi_name, ftr_install as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network from service_delivery)
,justrepairs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-tech' as journey_waypoint,'Repairs' as kpi_name, repairs as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network from service_delivery)
,mttr as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-tech' as journey_waypoint,'MTTR' as kpi_name, mttr as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network from service_delivery)
,ftrrepair as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'support-tech' as journey_waypoint,'%FTR_Repair' as kpi_name, ftr_repair as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network from service_delivery)
,repairs1k as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-tech' as journey_waypoint,'Repairs_per_1k_rgu' as kpi_name, Repairs_1k_rgu as kpi_meas, null as kpi_num,null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network from service_delivery)









############################################################## Join Flags ###########################################################################

,Join_DNA_KPIS as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network
  From( select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network From GrossAdds_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network From ActiveBase_Flag1
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network From ActiveBase_Flag2)
)

,Join_Sprints_KPIs as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network
  From( select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network From Join_DNA_kpis
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from TechTickets_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from MRCChanges_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from SalesSoftDx_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from EarlyIssues_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from LongInstall_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from EarlyTickets_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from RepeatedCall_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from TechCall1kRGU_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from CareCall1kRGU_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from BillingCallsPerBillVariation_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from FTRBilling_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from MountingBill_Flag
  )
)

,Join_New_KPIs as(
select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network
from( select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from join_sprints_kpis
--union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from payments
)
)

---NotCalculated kpis

--BUY

,ecommerce as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'digital_shift' as facet,'buy' as journey_waypoint,'%eCommerce' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network,extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

,tBuy as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'NPS_Detractorship' as facet,'buy' as journey_waypoint,'tBuy' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

,mttb as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'customer_time' as facet,'buy' as journey_waypoint,'MTTB' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

,Buyingcalls as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'contact_intensity' as facet,'buy' as journey_waypoint,'Buying_Calls/GA' as kpi_name, null as kpi_meas, null as kpi_num,null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

--GET

,tinstall as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'NPS_Detractorship' as facet,'get' as journey_waypoint,'tInstall' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

,selfinstalls as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'digital_shift' as facet,'get' as journey_waypoint,'%self_installs' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

,installscalls as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'contact_intensity' as facet,'get' as journey_waypoint,'Install_Calls/Installs' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

--PAY

,MTTBTR as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'customer_time' as facet,'pay' as journey_waypoint,'MTTBTR' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

,tpay as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'NPS_Detractorship' as facet,'pay' as journey_waypoint,'tpay' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

--Support-call
,helpcare as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'NPS_Detractorship' as facet,'support-call' as journey_waypoint,'tHelp_Care' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

,frccare as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'effectiveness' as facet,'support-call' as journey_waypoint,'%FRC_Care' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

--support-Tech

,helprepair as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'NPS_Detractorship' as facet,'support-tech' as journey_waypoint,'tHelp_repair' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

--use
,highrisk as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'high_risk' as facet,'use' as journey_waypoint,'%_High_Tech_Call_Nodes_+6%monthly' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

,pnps as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'NPS_Detractorship' as facet,'use' as journey_waypoint,'pNPS' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

--Wanda's Dashboard

,cccare as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'customer_time' as facet,'support-call' as journey_waypoint,'%CC_SL_Care' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

,cctech as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'customer_time' as facet,'support-call' as journey_waypoint,'%CC_SL_Tech' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

,chatbot as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'digital_shift' as facet,'support-call' as journey_waypoint,'%Chatbot_containment_care' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

,chahtbottech as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'digital_shift' as facet,'support-tech' as journey_waypoint,'%Chatbot_containment_Tech' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

,paymentsnull as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'digital_shift' as facet,'pay' as journey_waypoint,'%digital_payments' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,null as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)




,All_KPIs as(
select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network
from( select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from Join_Sprints_KPIs
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from ecommerce
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from tBuy
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from mttb
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from Buyingcalls

union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from MTTI
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from MTTR
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from tinstall
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from ftr_installs
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from installs
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from selfinstalls
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from installscalls

union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from MTTBTR
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from tpay

union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from helpcare
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from frccare

union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from helprepair
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from ftrrepair
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from justrepairs
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from repairs1k

union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from highrisk
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from pnps

union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from cccare
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from cctech
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from chatbot
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from chahtbottech
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from paymentsnull
))

,CX_Dashboard as(
Select Month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network,extract (year from date(Month)) as ref_year,extract(month from date(month)) as ref_mo
From All_KPIs
)

--,OverallKPIs as(
  Select Distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name, CASE 
  WHEN kpi_num IS NULL AND kpi_den IS NULL THEN sum(kpi_meas)
  WHEN kpi_num IS NOT NULL AND kpi_den IS NOT NULL THEN safe_divide(sum(kpi_num),sum(kpi_den))
  ELSE NULL END AS kpi_meas,
  ifnull(sum(kpi_num),0) as kpi_numm,
  ifnull(sum(kpi_den),0) as kpi_denn,
  "Overall" as Network,
  extract (year from date(Month)) as ref_year,extract(month from date(month)) as ref_mo
  From CX_Dashboard
  group by 1,2,3,4,5,6,7,8,9,kpi_num,kpi_den
--)
