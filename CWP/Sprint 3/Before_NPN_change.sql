-----------------------------------------------------------------------------------------
------------------------- SPRINT 3 PARAMETRIZADO - V1 -----------------------------------
-----------------------------------------------------------------------------------------

WITH

parameters AS (
-- Seleccionar el mes en que se desea realizar la corrida
SELECT DATE_TRUNC('month',DATE('2022-09-01')) AS input_month
)

,fmc_table AS (
SELECT * FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod"
WHERE month = DATE(dt) AND month = (SELECT input_month FROM parameters)
)
----------------------- New Customers -------------------------------------
,previous_months_dna AS (
-- Se guardan las cuentas que aparecen en los 3 meses anteriores
SELECT  DATE_TRUNC('month',CAST(dt AS DATE)) AS month
        ,act_acct_cd
FROM "db-analytics-prod"."fixed_cwp"
WHERE act_cust_typ_nm = 'Residencial'
        AND DATE_TRUNC('month',DATE(dt)) BETWEEN ((SELECT input_month FROM parameters) - interval '3' month) AND ((SELECT input_month FROM parameters) - interval '1' month)
GROUP BY 1,2
ORDER BY 1,2
)

,new_customers AS (
-- Se seleccionan los usuarios que aparecen en el current month pero no en los tres anteriores para flagearlos como new customers
SELECT  act_acct_cd
        ,DATE(dt) AS dt
        ,DATE_TRUNC('MONTH',CAST(dt AS DATE)) AS month_load
        ,DATE_TRUNC('MONTH',CAST(act_cust_strt_dt AS DATE)) AS month_start
        ,CAST(SUBSTR(pd_mix_cd,1,1) AS INT) AS n_rgu
        ,max(act_acct_inst_dt) AS act_acct_inst_dt 
        ,max(act_cust_strt_dt) AS act_cust_strt_dt
        ,1 AS new_customer
        ,pd_bb_accs_media
        ,pd_tv_accs_media
        ,pd_vo_accs_media
FROM "db-analytics-prod"."fixed_cwp"
WHERE act_cust_typ_nm = 'Residencial'
        AND act_acct_cd  NOT IN (SELECT DISTINCT act_acct_cd FROM previous_months_dna)
        AND DATE_TRUNC('month',CAST(dt AS DATE)) = (SELECT input_month FROM parameters)
GROUP BY act_acct_cd, 2, DATE_TRUNC('MONTH',CAST(dt AS DATE)),CAST(act_cust_strt_dt AS DATE),
CAST(SUBSTR(pd_mix_cd,1,1) AS INT),1, pd_bb_accs_media,pd_tv_accs_media,pd_vo_accs_media
)

,new_customers_flag AS (
SELECT  F.*
        ,A.new_customer 
        ,CASE   WHEN F.first_sales_chnl_bom IS NOT NULL AND F.first_sales_chnl_eom IS NOT NULL THEN F.first_sales_chnl_eom
                WHEN F.first_sales_chnl_bom IS NULL AND F.first_sales_chnl_eom is not null then F.first_sales_chnl_eom
                WHEN F.first_sales_chnl_eom IS NULL AND F.first_sales_chnl_bom is not null then F.first_sales_chnl_bom
                END as sales_channel
        ,CASE   WHEN a.act_acct_cd IS NOT NULL THEN 1 ELSE 0 END AS monthsale_flag
FROM fmc_table F LEFT JOIN new_customers A ON F.finalaccount = A.act_acct_cd AND F.month = A.month_load
)
----------------------- INTERACTIONS --------------------------------------
,clean_interaction_time AS (
SELECT *
FROM "db-stage-prod"."interactions_cwp"
WHERE (CAST(interaction_start_time AS VARCHAR) != ' ')
    AND interaction_start_time IS NOT NULL
    AND DATE_TRUNC('month',CAST(SUBSTR(CAST(interaction_start_time AS VARCHAR),1,10) AS DATE)) BETWEEN ((SELECT input_month FROM parameters)) AND ((SELECT input_month FROM parameters) + interval '2' month)
)

,interactions_inicial AS (
SELECT  *
        ,CAST(SUBSTR(CAST(interaction_start_time AS VARCHAR),1,10) AS DATE) AS interaction_date, DATE_TRUNC('month',CAST(SUBSTR(CAST(interaction_start_time AS VARCHAR),1,10) AS DATE)) AS month
        ,CASE   WHEN interaction_purpose_descrip = 'CLAIM' AND interaction_disposition_info LIKE '%retenci%n%' THEN 'retention_claim'	 
                WHEN interaction_purpose_descrip = 'CLAIM' AND interaction_disposition_info LIKE '%restringido%' THEN 'service_restriction_claim'		
                WHEN interaction_purpose_descrip = 'CLAIM' AND interaction_disposition_info LIKE '%instalacion%' THEN 'installation_claim'
                WHEN interaction_purpose_descrip = 'CLAIM' AND (
                        interaction_disposition_info LIKE '%afiliacion%factura%' OR
                        interaction_disposition_info LIKE '%cliente%desc%' OR
                        interaction_disposition_info LIKE '%consulta%cuentas%' OR
                        interaction_disposition_info LIKE '%consulta%productos%' OR
                        interaction_disposition_info LIKE '%consumo%' OR
                        interaction_disposition_info LIKE '%info%cuenta%productos%' OR
                        interaction_disposition_info LIKE '%informacion%general%' OR
                        interaction_disposition_info LIKE '%pagar%on%line%' OR
                        interaction_disposition_info LIKE '%saldo%' OR
                        interaction_disposition_info LIKE '%actualizacion%datos%' OR
                        interaction_disposition_info LIKE '%traslado%linea%' OR
                        interaction_disposition_info LIKE '%transfe%cta%'
                        ) THEN 'account_info_or_balance_claim'
                WHEN interaction_purpose_descrip = 'CLAIM' AND (
                        interaction_disposition_info LIKE '%cargo%' OR
                        interaction_disposition_info LIKE '%credito%' OR
                        interaction_disposition_info LIKE '%facturaci%n%' OR
                        interaction_disposition_info LIKE '%pago%' OR
                        interaction_disposition_info LIKE '%prorrateo%' OR
                        interaction_disposition_info LIKE '%alto%consumo%' OR
                        interaction_disposition_info LIKE '%investigacion%interna%' OR
                        interaction_disposition_info LIKE '%cambio%descuento%'
                        ) THEN 'billing_claim'	
                WHEN interaction_purpose_descrip = 'CLAIM' AND (
                        interaction_disposition_info LIKE '%venta%' OR
                        interaction_disposition_info LIKE '%publicidad%' OR
                        interaction_disposition_info LIKE '%queja%promo%precio%' OR
                        interaction_disposition_info LIKE '%promo%' OR
                        interaction_disposition_info LIKE '%promo%' OR
                        interaction_disposition_info LIKE '%cambio%de%precio%' OR  
                        interaction_disposition_info LIKE '%activacion%producto%' OR
                        interaction_disposition_info LIKE '%productos%servicios%-internet%' OR
                        interaction_disposition_info LIKE '%productos%servicios%suplementarios%' OR
                        interaction_disposition_info LIKE '%paytv%-tv%digital%hd%'
                        ) THEN 'sales_claim'	
                WHEN interaction_purpose_descrip = 'CLAIM' AND (
                        interaction_disposition_info LIKE '%queja%mala%atencion%' OR
                        interaction_disposition_info LIKE '%dunning%' OR
                        interaction_disposition_info LIKE '%consulta%reclamo%' OR
                        interaction_disposition_info LIKE '%apertura%reclamo%' OR
                        interaction_disposition_info LIKE '%horario%tienda%' OR
                        interaction_disposition_info LIKE '%consulta%descuento%' OR
                        interaction_disposition_info LIKE '%cuenta%apertura%reclamo%' OR
                        interaction_disposition_info LIKE '%actualizar%apc%' OR
                        interaction_disposition_info LIKE '%felicita%' 
                        ) THEN 'customer_service_claim'
                WHEN interaction_purpose_descrip = 'CLAIM' AND (
                        interaction_disposition_info LIKE '% da%no%' OR
                        interaction_disposition_info LIKE '%-da%no%' OR
                        interaction_disposition_info LIKE '%servicios%-da%os%' OR
                        interaction_disposition_info LIKE '%datos%internet%' OR
                        interaction_disposition_info LIKE '%equipo%recuperado%' OR
                        interaction_disposition_info LIKE '%escalami%niv%' OR
                        interaction_disposition_info LIKE '%queja%internet%' OR
                        interaction_disposition_info LIKE '%queja%linea%' OR
                        interaction_disposition_info LIKE '%queja%tv%' OR
                        interaction_disposition_info LIKE '%reclamos%tv%' OR
                        interaction_disposition_info LIKE '%serv%func%' OR
                        interaction_disposition_info LIKE '%soporte%internet%' OR
                        interaction_disposition_info LIKE '%soporte%linea%' OR
                        interaction_disposition_info LIKE '%tecn%' OR
                        interaction_disposition_info LIKE '%no%casa%internet%' OR
                        interaction_disposition_info LIKE '%no%energia%internet%' OR
                        interaction_disposition_info LIKE '%soporte%' OR
                        interaction_disposition_info LIKE '%intermiten%' OR
                        interaction_disposition_info LIKE '%masivo%'
                        ) THEN 'technical_claim'	
                WHEN interaction_purpose_descrip = 'CLAIM' THEN 'other_claims'							
                WHEN interaction_purpose_descrip = 'TICKET' THEN 'tech_ticket'							
                WHEN interaction_purpose_descrip = 'TRUCKROLL' THEN 'tech_truckroll'							
                END AS interact_category
FROM    (SELECT *,
        CASE    WHEN interaction_purpose_descrip = 'CLAIM' THEN REPLACE(CONCAT(LOWER(COALESCE(other_interaction_info4,' ')),'-',LOWER(COALESCE(other_interaction_info5,' '))),'  ','') 
                WHEN (interaction_purpose_descrip = 'TICKET' OR interaction_purpose_descrip = 'TRUCKROLL') THEN REPLACE(CONCAT(LOWER(COALESCE(other_interaction_info8,' ')),'-',LOWER(COALESCE(other_interaction_info9,' '))),'  ','') ELSE NULL END AS interaction_disposition_info
        FROM clean_interaction_time
        WHERE interaction_id IS NOT NULL 
        )
)

,interactions AS (
SELECT  * 
        ,CASE   WHEN interact_category = 'billing_claim' THEN 'Billing'
                WHEN interact_category = 'account_info_or_balance_claim' THEN 'Account Info'
                WHEN interact_category = 'retention_claim' THEN 'Retention'
                WHEN interact_category IN ('installation_claim','tech_ticket','tech_truckroll','technical_claim') THEN 'Technical'
                ELSE 'Others' END AS interaction_type
FROM interactions_inicial
)
----------------------- Soft DX A9 and Never Pay A13 ---------------------
,union_dna AS (
SELECT  act_acct_cd
        ,fi_outst_age
        ,DATE(dt) AS dt
        ,pd_mix_cd
        ,pd_bb_accs_media
        ,pd_TV_accs_media
        ,pd_VO_accs_media
        ,act_acct_inst_dt
        ,act_cust_strt_dt
        ,act_cust_typ_nm
        ,DATE_TRUNC('month',DATE(dt)) AS month_load
        ,fi_bill_dt_m0
        ,fi_bill_dt_m1
        ,fi_bill_due_dt_m1
        ,fi_bill_due_dt_m0
        ,fi_bill_dt_m2
        ,fi_bill_due_dt_m2
FROM "db-analytics-prod"."fixed_cwp"
WHERE act_cust_typ_nm = 'Residencial'
    AND CAST(fi_outst_age AS BIGINT) <= 95 OR fi_outst_age IS NULL
    AND DATE_TRUNC('month',DATE(act_acct_inst_dt)) BETWEEN ((SELECT input_month FROM parameters) - interval '2' month) AND ((SELECT input_month FROM parameters) + interval '1' month)
)

,monthly_inst_accounts AS (
SELECT  act_acct_cd
        ,DATE_TRUNC('month',DATE(act_acct_inst_dt)) AS inst_month
FROM union_dna
WHERE act_cust_typ_nm = 'Residencial' 
    AND DATE_TRUNC('month',DATE(act_acct_inst_dt)) = month_load
)

,first_bill AS (
SELECT  act_acct_cd
        ,CONCAT(MAX(act_acct_cd),'-',MIN(first_oldest_unpaid_bill_dt)) AS act_first_bill
        ,DATE_TRUNC('month',first_inst_dt) AS inst_month
FROM    (SELECT act_acct_cd
                ,FIRST_VALUE(DATE(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_inst_dt 
                ,FIRST_VALUE(oldest_unpaid_bill_dt) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_oldest_unpaid_bill_dt
        FROM    (SELECT act_acct_cd
                        ,fi_outst_age
                        ,DATE(dt) AS dt
                        ,act_acct_inst_dt
                        ,CASE   WHEN fi_outst_age IS NULL THEN '1900-01-01' 
                                ELSE CAST(DATE_ADD('day',-CAST(fi_outst_age AS INT),DATE(dt)) AS VARCHAR)
                                END AS oldest_unpaid_bill_dt
                FROM union_dna
                WHERE act_cust_typ_nm = 'Residencial'
                    AND act_acct_cd IN (SELECT act_acct_cd FROM monthly_inst_accounts)
                    AND DATE(dt) BETWEEN ((DATE_TRUNC('month',DATE(act_cust_strt_dt))) - interval '12' month) AND ((DATE_TRUNC('month',date(act_cust_strt_dt))) + interval '6' month)
                )
        WHERE oldest_unpaid_bill_dt <> '1900-01-01'
        )
GROUP BY act_acct_cd,3
)

,max_overdue_first_bill AS (
SELECT  act_acct_cd
        ,DATE_TRUNC('month',DATE(MIN(first_inst_dt))) AS month_inst
        ,MIN(DATE(first_oldest_unpaid_bill_dt)) AS first_oldest_unpaid_bill_dt
        ,MIN(first_inst_dt) AS first_inst_dt
        ,MIN(first_act_cust_strt_dt) AS first_act_cust_strt_dt
        ,CONCAT(MAX(act_acct_cd),'-',MIN(first_oldest_unpaid_bill_dt)) AS act_first_bill
        ,MAX(fi_outst_age) AS max_fi_outst_age
        ,MAX(fi_overdue_age) AS max_fi_overdue_age
        ,MAX(DATE(dt)) AS max_dt
        ,CASE WHEN MAX(CAST(fi_outst_age AS INT)) >= 90 THEN 1 ELSE 0 END AS hard_dx_flg
FROM    (SELECT act_acct_cd
                ,FIRST_VALUE(oldest_unpaid_bill_dt) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_oldest_unpaid_bill_dt
                ,FIRST_VALUE(DATE(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_inst_dt
                ,FIRST_VALUE(DATE(act_cust_strt_dt)) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_act_cust_strt_dt
                ,fi_outst_age
                ,DATE(dt) AS dt
                ,pd_mix_cd
                ,fi_overdue_age
        FROM    (SELECT act_acct_cd
                        ,fi_outst_age
                        ,DATE(dt) AS dt
                        ,pd_mix_cd
                        ,pd_bb_accs_media
                        ,pd_TV_accs_media
                        ,pd_VO_accs_media
                        ,act_acct_inst_dt
                        ,act_cust_strt_dt
                        ,CASE   WHEN fi_outst_age IS NULL THEN '1900-01-01'
                                ELSE CAST(DATE_ADD('day',-CAST(fi_outst_age AS INT),DATE(dt)) AS VARCHAR) 
                                END AS oldest_unpaid_bill_dt
                        ,CASE   WHEN fi_bill_dt_m0 IS NOT NULL THEN CAST(fi_outst_age AS INT) - DATE_DIFF('day',DATE(fi_bill_dt_m0),DATE(fi_bill_due_dt_m0))
                                WHEN fi_bill_dt_m1 IS NOT NULL THEN CAST(fi_outst_age AS INT) - DATE_DIFF('day',DATE(fi_bill_dt_m1),DATE(fi_bill_due_dt_m1))
                                ELSE CAST(fi_outst_age AS INT) - DATE_DIFF('day',DATE(fi_bill_dt_m2),DATE(fi_bill_due_dt_m2))
                                END AS fi_overdue_age
                        FROM union_dna
                        WHERE act_cust_typ_nm = 'Residencial'
                            AND act_acct_cd IN (SELECT act_acct_cd FROM monthly_inst_accounts)
                            AND DATE(dt) BETWEEN (DATE_TRUNC('month',DATE(act_acct_inst_dt))) AND ((DATE_TRUNC('month',DATE(act_acct_inst_dt))) + interval '5' month) 
                )
        WHERE CONCAT(act_acct_cd,'-',oldest_unpaid_bill_dt) IN (SELECT act_first_bill FROM first_bill) 
        )
GROUP BY act_acct_cd
)

,sft_hard_dx AS (
SELECT  * 
        ,DATE_ADD('day',(46),first_oldest_unpaid_bill_dt) AS threshold_pay_date
        ,CASE WHEN (max_fi_outst_age >= 46 AND month_inst < DATE('2022-05-01')) OR (max_fi_overdue_age >= 5 AND month_inst >= DATE('2022-05-01')) THEN 1 ELSE 0 END AS soft_dx_flg
        ,CASE WHEN DATE_ADD('day',(46),first_oldest_unpaid_bill_dt) < current_date THEN 1 ELSE 0 END AS soft_dx_window_completed
        ,CASE WHEN DATE_ADD('day',(90),first_oldest_unpaid_bill_dt) < current_date THEN 1 ELSE 0 END AS never_paid_window_completed
        ,current_date AS current_date_analysis
FROM max_overdue_first_bill
)

,join_dx_new_customers AS (
SELECT  A.month_load
        ,A.act_acct_cd
        ,soft_dx_flg AS soft_dx
        ,hard_dx_flg AS hard_dx
FROM new_customers A LEFT JOIN sft_hard_dx B ON A.act_acct_cd = B.act_acct_cd
)

,flag_soft_hard_dx AS (
SELECT  F.*
        ,soft_dx
        ,hard_dx
        ,CASE WHEN soft_dx = 1 THEN 1 ELSE NULL END AS straight_soft_dx_flag
        ,CASE WHEN hard_dx = 1 THEN 1 ELSE null END AS NEVER_PAID_FLAG
FROM new_customers_flag F LEFT JOIN join_dx_new_customers A ON F.finalaccount = A.act_acct_cd AND F.month = A.month_load
)
------------------------ LATE INSTALLATIONS B1 ---------------------------
,service_orders AS (
SELECT  *
        ,DATE_TRUNC('Month', DATE(order_start_date)) AS month
        ,DATE(order_start_date) AS StartDate
        ,DATE(completed_date) AS EndDate
        ,DATE_DIFF('DAY',DATE(order_start_date),DATE(completed_date)) AS installation_lapse
FROM "db-stage-dev"."so_hdr_cwp"
WHERE order_type = 'INSTALLATION' AND ACCOUNT_TYPE='R' AND ORDER_STATUS='COMPLETED'
        AND DATE_TRUNC('MONTH',CAST(order_start_date AS DATE)) = (SELECT input_month FROM parameters)
)

,late_installation_flag AS (
SELECT  A.* 
        ,CASE WHEN B.installation_lapse > 5 THEN 1 ELSE 0 END AS late_inst_flag
FROM flag_soft_hard_dx A LEFT JOIN service_orders B on A.month = B.month AND CAST(A.finalaccount AS VARCHAR) = CAST(B.account_id AS VARCHAR)
)
------------------------ EARLY INTERACTION B4 ----------------------------
,new_customer_interactions_info AS (
SELECT  A.act_acct_cd
        ,B.account_id
        ,A.month_load
        ,CASE WHEN account_id IS NOT NULL THEN 1 ELSE 0 END AS early_interaction_flag
FROM new_customers A LEFT JOIN interactions B ON A.act_acct_cd = B.account_id
WHERE DATE_DIFF('DAY',CAST(act_acct_inst_dt AS DATE),CAST(interaction_date AS DATE)) <= 21
    AND interaction_type = 'Technical'
GROUP BY 1,2,3
)


,early_int_flag AS (
SELECT  F.*
        ,early_interaction_flag
FROM late_installation_flag F LEFT JOIN new_customer_interactions_info A ON F.finalaccount = A.act_acct_cd AND F.month = A.month_load
)
------------------------ EARLY TICKET B9 ---------------------------------
,early_ticket_info AS (
SELECT  F.account_id
        ,F.month
        ,CASE WHEN F.account_id IS NOT NULL THEN 1 ELSE NULL END AS early_ticket_flag
FROM new_customers E INNER JOIN interactions F ON E.act_acct_cd = F.account_id
WHERE DATE_DIFF('week',CAST(act_acct_inst_dt AS DATE),CAST(interaction_date AS DATE)) <= 7
    AND interaction_purpose_descrip = 'TICKET'
GROUP BY account_id, month
)

,early_tkt_flag AS (
SELECT  F.*
        ,early_ticket_flag
FROM early_int_flag F LEFT JOIN early_ticket_info A ON F.finalaccount = A.account_id AND F.month = A.month
)
------------------------ Billing CLaims C6 -------------------------------
,stock_key AS (
SELECT  * 
        ,CONCAT(finalaccount,SUBSTR(CAST(month AS VARCHAR),1,7)) AS key_stock
FROM early_tkt_flag 
)

,customers_billing_claims AS (
SELECT  CONCAT(account_id, SUBSTR(CAST(DATE_TRUNC('Month',interaction_date) AS VARCHAR),1,7)) AS key_bill_claims
FROM interactions
WHERE interaction_type = 'Billing'
)

,billing_claims_flag AS (
SELECT  A.*
        ,CASE WHEN key_bill_claims IS NOT NULL THEN 1 ELSE 0 END AS bill_claim_flag
FROM stock_key A LEFT JOIN customers_billing_claims B ON A.key_stock = B.key_bill_claims
)
------------------------ MRC Changes -------------------------------------
,mrc_changes AS (
SELECT  CONCAT(act_acct_cd, SUBSTR(CAST(DATE_TRUNC('Month',DATE(dt)) AS VARCHAR),1,7)) AS key_mrc_changes
        ,((fi_tot_mrc_amt-fi_tot_mrc_amt_prev)/fi_tot_mrc_amt_prev) AS mrc_change
FROM  "db-analytics-prod"."fixed_cwp"
WHERE pd_vo_prod_nm_prev = pd_vo_prod_nm
    AND pd_bb_prod_nm_prev = pd_bb_prod_nm
    AND pd_tv_prod_nm_prev = pd_tv_prod_nm
    AND DATE_TRUNC('month',DATE(dt)) BETWEEN ((SELECT input_month FROM parameters) - interval '2' month) AND ((SELECT input_month FROM parameters) + interval '1' month)
--GROUP BY 1,2
)

,join_mrc_change AS (
SELECT  A.*
        ,B.mrc_change
FROM billing_claims_flag A LEFT JOIN mrc_changes B ON A.key_stock = B.key_mrc_changes
)

,change_mrc_flag AS (
SELECT  *
        ,CASE WHEN mrc_change > 0.05 OR mrc_change < -0.05 THEN 1 ELSE 0 END AS mrc_change_flag
        ,CASE WHEN mrc_change IS NOT NULL THEN finalaccount ELSE NULL END AS no_plan_change
FROM join_mrc_change
)
------------------------ Mounting Bills ----------------------------------
,overdue_records AS (
SELECT  DATE_TRUNC('Month',CAST(dt AS DATE)) AS month
        ,act_acct_cd 
        ,CASE WHEN fi_outst_age IS NULL THEN -1 ELSE fi_outst_age END AS fi_outst_age
        ,CASE WHEN fi_outst_age = 60 THEN 1 ELSE 0 END AS day_60
        ,first_value(CASE WHEN fi_outst_age IS NULL THEN -1 ELSE fi_outst_age END) IGNORE NULLS OVER(PARTITION BY DATE_TRUNC('Month',CAST(dt AS DATE)), act_acct_cd ORDER BY dt DESC) AS last_overdue_record
        ,first_value(CASE WHEN fi_outst_age IS NULL THEN -1 ELSE fi_outst_age END) IGNORE NULLS OVER(PARTITION BY DATE_TRUNC('Month',CAST(dt AS DATE)), act_acct_cd ORDER BY dt) AS first_overdue_record
from "db-analytics-prod"."fixed_cwp"
WHERE act_cust_typ_nm = 'Residencial' 
    AND CAST(dt AS DATE) BETWEEN DATE_TRUNC('MONTH',CAST(dt AS DATE)) AND DATE_ADD('MONTH',1,DATE_TRUNC('MONTH',CAST(dt AS DATE)))
)

,grouped_overdue_records AS (
SELECT  month
        ,act_acct_cd
        ,max(fi_outst_age) AS max_overdue
        ,max(day_60) AS mounting_bill_flag
        ,max(last_overdue_record) AS last_overdue_record
        ,max(first_overdue_record) AS first_overdue_record
FROM overdue_records
GROUP BY 1,2
)

,mounting_bills_flag AS (
SELECT  F.*
        ,B.mounting_bill_flag
FROM change_mrc_flag F LEFT JOIN grouped_overdue_records B ON F.finalaccount = B.act_acct_cd AND F.month = B.month
)

,all_flags AS (
SELECT * 
FROM mounting_bills_flag
)
------------------------ FLAGS TABLE -------------------------------------
,FullTable_KPIsFlags AS (
SELECT  *
        ,CASE WHEN monthsale_flag = 1 THEN CONCAT(CAST(monthsale_flag AS VARCHAR), fixedaccount) ELSE NULL END AS F_SalesFlag
        ,CASE WHEN straight_soft_dx_flag = 1 AND new_customer = 1 THEN CONCAT(CAST(straight_soft_dx_flag AS VARCHAR), fixedaccount) ELSE NULL END AS F_SoftDxFlag
        ,CASE WHEN never_paid_flag = 1 THEN CONCAT(CAST(never_paid_flag AS VARCHAR), fixedaccount) ELSE NULL END AS F_NeverPaidFlag
        ,CASE WHEN late_inst_flag = 1 AND new_customer = 1 THEN CONCAT(CAST(late_inst_flag AS VARCHAR), fixedaccount) ELSE NULL END AS F_LongInstallFlag
        ,CASE WHEN early_interaction_flag = 1 AND new_customer = 1 THEN CONCAT(CAST(early_interaction_flag AS VARCHAR), fixedaccount) ELSE NULL END AS F_EarlyInteractionFlag
        ,CASE WHEN early_ticket_flag = 1 AND new_customer = 1 THEN CONCAT(CAST(early_ticket_flag AS VARCHAR), fixedaccount) ELSE NULL END AS F_EarlyTicketFlag
        ,CASE WHEN bill_claim_flag = 1 THEN CONCAT(CAST(bill_claim_flag AS VARCHAR), fixedaccount) ELSE NULL END AS F_BillClaim
        ,CASE WHEN mrc_change_flag = 1 THEN CONCAT(CAST(mrc_change_flag AS VARCHAR), fixedaccount) ELSE NULL END AS F_MRCChange
        ,CASE WHEN Mounting_bill_flag = 1 THEN CONCAT(CAST(mounting_bill_flag AS VARCHAR), fixedaccount) ELSE NULL END AS F_MountingBillFlag
FROM all_flags
)

,saleschannel_so AS (
SELECT  month
        ,channel_desc
        ,account_id
        ,CASE   WHEN channel_desc IN ('Provincia de Chiriqui','PROM','VTASE','PHs1','Busitos','Alianza','Coronado','Ventas Externas/ADSL','PHs 2') OR channel_desc LIKE '%PROM%' OR channel_desc LIKE '%VTASE%' OR channel_desc LIKE '%Busitos%' OR channel_desc LIKE '%Alianza%' THEN 'D2D (Own Sales force)'
                WHEN channel_desc IN ('Dinamo','Oficinista','Distribuidora Arandele','Orde Technology','SLAND','SI Panamá') THEN 'D2D (Outsourcing)'
                WHEN channel_desc IN ('Vendedores','Metro Mall','WESTLAND MALL','TELEMART AGUADULCE') THEN 'Retail (Own Stores)'
                WHEN channel_desc IN (/*'Telefono',*/'123 Outbound','Gestión') OR channel_desc LIKE '%Gestión%' OR channel_desc LIKE '%Gestion%' THEN 'Outbound – TeleSales'
                WHEN channel_desc IN ('Centro de Retencion','Centro de Llamadas','Call Cnter MULTICALL') THEN 'Inbound – TeleSales'
                WHEN channel_desc IN ('Nestrix','Tienda OnLine','Live Person','Telefono') THEN 'Digital'
                WHEN channel_desc IN ('Panafoto Dorado','Agencia') OR channel_desc LIKE '%Agencia%' OR channel_desc LIKE '%AGENCIA%' THEN 'Retail (Distributer-Dealer)'
                WHEN channel_desc IN ('CIS+ GUI','Solo para uso de IT','Apuntate',' CU2Si','RC0E Collection','Carta','Proyecto','DE=Demo','Recarga saldo','Port Postventa','Feria','Administracion','Postventa-verif.orde','No Factibilidad','Orden a construir','Inversiones AP','Promotor','VIVI MAS') OR channel_desc LIKE '%Feria%' THEN 'Not a Sales Channel'
                END AS sales_channel_SO
FROM    (SELECT DISTINCT DATE_TRUNC('Month',DATE(completed_date)) AS month
                ,account_id
                ,first_value(channel_desc) OVER (PARTITION BY account_id ORDER BY order_start_date) AS channel_desc
        FROM "db-stage-dev"."so_hdr_cwp" 
        WHERE order_type ='INSTALLATION' AND DATE_TRUNC('month',CAST(completed_date AS DATE)) = (SELECT input_month FROM parameters)
        )
)

,FullTable_Adj AS (
SELECT  F.*
        ,sales_channel_SO
FROM FullTable_KPIsFlags F LEFT JOIN saleschannel_so S ON F.fixedaccount = CAST(S.account_id AS VARCHAR)
)
------------------------ RESULTS QUERY -----------------------------------

SELECT  month
        ,B_Final_TechFlag
        ,B_FMCSegment
        ,E_Final_TechFlag
        ,E_FMCSegment
        ,b_final_tenure
        ,e_final_tenure
        ,B_FixedTenure
        ,E_FixedTenure
        ,COUNT(DISTINCT fixedaccount) AS activebase
        ,SUM(monthsale_flag) AS Sales
        ,SUM(straight_soft_dx_flag) AS Soft_Dx
        ,SUM(Never_Paid_Flag) AS NeverPaid
        ,SUM(late_inst_flag) AS Long_installs
        ,SUM(early_interaction_flag) AS Early_Issues
        ,SUM(early_ticket_flag) AS Early_ticket
        ,COUNT(DISTINCT F_SalesFlag) AS Unique_Sales
        ,COUNT(DISTINCT F_SoftDxFlag) AS Unique_SoftDx
        ,COUNT(DISTINCT F_NeverPaidFlag) AS Unique_NeverPaid
        ,COUNT(DISTINCT F_LongInstallFlag) AS Unique_LongInstall
        ,COUNT(DISTINCT F_EarlyInteractionFlag) AS Unique_EarlyInteraction
        ,COUNT(DISTINCT F_EarlyTicketFlag) AS Unique_EarlyTicket
        ,COUNT(DISTINCT F_BillClaim) AS Unique_BillClaim
        ,COUNT(DISTINCT no_plan_change) AS NoPlan
        ,COUNT(DISTINCT F_MRCChange) AS Unique_MRCChange
        ,COUNT(DISTINCT F_MountingBillFlag) AS Unique_MountingBill
        ,B_FMCTYPE
        ,E_FMCTYPE
        ,first_sales_chnl_eom
        ,first_sales_chnl_bom
        ,Last_Sales_CHNL_EOM
        ,Last_Sales_CHNL_BOM 
        ,sales_channel
        ,sales_channel_so
FROM FullTable_Adj
WHERE ((Fixedchurntype != 'Fixed Voluntary Churner' AND Fixedchurntype != 'Fixed Involuntary Churner') OR Fixedchurntype IS NULL) AND finalchurnflag !='Fixed Churner'
GROUP BY 1,2,3,4,5,6,7,8,9,B_FMCTYPE, E_FMCTYPE, first_sales_chnl_eom, first_sales_chnl_bom, Last_Sales_CHNL_EOM, Last_Sales_CHNL_BOM , sales_channel,sales_channel_so
ORDER BY 1
