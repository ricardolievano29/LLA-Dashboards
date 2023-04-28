WITH 
Parameters AS (
SELECT DATE('2023-03-01') AS input_month
)
--################################## INFORMACIÓN GENERAL #######################################

,sales_channel_calculation AS (
/*Es importante dejar clara la regla de atribución de sales channel, si un usuario tiene diferentes canales de ventas en el mes de consulta se atribuirá la venta al primero de estos*/
SELECT  CAST(cuenta AS VARCHAR) AS act_acct_cd 
        ,TRY(ARRAY_AGG(sales_channel ORDER BY DATE(sales_month)) [1]) as fi_sales_channel
        ,TRY(ARRAY_AGG(sales_channel_sub ORDER BY DATE(sales_month)) [1]) as fi_sales_channel_sub
        ,TRY(ARRAY_AGG(codigo_de_vendedor ORDER BY DATE(sales_month)) [1]) as fi_codigo_de_vendedor
        ,TRY(ARRAY_AGG(nombre_vendedor ORDER BY DATE(sales_month)) [1]) as fi_nombre_vendedor
        ,TRY(ARRAY_AGG(CAST(rgus_vendidos AS INT) ORDER BY DATE(sales_month)) [1]) as fi_rgu_vendidos
FROM "db-stage-prod"."cwp_gross_adds_fijo" 
WHERE sales_month != '' and sales_month IS NOT NULL
     AND DATE_TRUNC('MONTH', DATE(sales_month)) = (SELECT input_month FROM Parameters)
GROUP BY cuenta
)

,candidates_sales as (
SELECT *
FROM ( 
    SELECT act_acct_cd, min(date(dt)) AS first_dna_date
    FROM "db-analytics-prod"."fixed_cwp"
    WHERE act_cust_typ_nm = 'Residencial'
        --AND DATE_TRUNC('MONTH', act_acct_inst_dt) = (SELECT input_month FROM Parameters) /*Debemos ajustar este criterio para que sea sobre el DT*/
        AND DATE(dt) BETWEEN (SELECT input_month FROM Parameters) - interval '6' MONTH AND  (SELECT input_month FROM Parameters) + INTERVAL '1' MONTH
    GROUP BY act_acct_cd
    ) 
WHERE date_trunc('month', first_dna_date) = (SELECT input_month FROM parameters)
)

,dna_usefull_fields AS (
    SELECT  act_acct_cd
            --,FIRST_VALUE(DATE(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY act_acct_inst_dt) AS first_installation_date
            ,fi_outst_age
            ,fi_tot_mrc_amt
            ,DATE(dt) AS dt
            ,pd_mix_cd
            ,CASE 
                WHEN pd_mix_cd = '1P' THEN 1 
                WHEN pd_mix_cd = '2P' THEN 2 
                WHEN pd_mix_cd = '3P' THEN 3 
                ELSE 0 
            END AS pd_mix_qty
            ,pd_bb_prod_nm
            ,pd_tv_prod_nm
            ,pd_vo_prod_nm
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
            ,CASE   
                WHEN fi_outst_age IS NULL THEN date('1900-01-01' )
                ELSE date(CAST(DATE_ADD('day',-CAST(fi_outst_age AS INT),DATE(dt)) AS VARCHAR))
            END AS oldest_unpaid_bill_dt
    FROM "db-analytics-prod"."fixed_cwp"
    WHERE act_cust_typ_nm = 'Residencial'
    AND DATE(dt) between  (SELECT input_month FROM Parameters) AND  (SELECT input_month FROM Parameters) + INTERVAL '4' MONTH 
)

,first_installation_calculation AS (
SELECT dna.act_acct_cd
        -- ,MIN(cs.first_dna_date) AS first_dna_date
        ,TRY(FILTER(ARRAY_AGG(IF(DATE(act_acct_inst_dt) >= cs.first_dna_date, DATE(act_acct_inst_dt), NULL) ORDER BY act_acct_inst_dt), X-> X IS NOT NULL)[1]) AS first_installation_date
FROM dna_usefull_fields as dna
INNER JOIN candidates_sales as cs on cs.act_acct_cd = dna.act_acct_cd
GROUP BY dna.act_acct_cd
)

,main_info_sales_base AS (
SELECT *
FROM sales_channel_calculation AS b
INNER JOIN dna_usefull_fields AS a USING(act_acct_cd) 
)


--###################### INFORMACIÓN PARA NPN DE GROSS ADDS ####################################
,sales_base AS (
SELECT *
FROM main_info_sales_base AS a
INNER JOIN candidates_sales AS b USING(act_acct_cd)
LEFT JOIN first_installation_calculation AS c USING (act_acct_cd)
) 

,bills_of_interest AS (
SELECT act_acct_cd,
    /*Usamos la fecha de la primera factura generada y no del oldes_unpaid_bill para no ser susceptibles a errores en el fi_outst_age o oldet_unpaid_bill*/
    DATE(TRY(FILTER(ARRAY_AGG(fi_bill_dt_m0 ORDER BY DATE(dt)), x -> x IS NOT NULL)[1])) AS first_bill_created
FROM sales_base
GROUP BY act_acct_cd 
)

,mrc_calculation AS (
SELECT sb.act_acct_cd, 
        MIN(bi.first_bill_created) AS first_bill_created, 
        MAX(fi_tot_mrc_amt) AS max_tot_mrc, 
        ARRAY_AGG(DISTINCT fi_tot_mrc_amt ORDER BY fi_tot_mrc_amt DESC) AS ARREGLO_MRC
FROM sales_base AS sb
INNER JOIN bills_of_interest AS bi ON sb.act_acct_cd = bi.act_acct_cd AND sb.dt BETWEEN first_bill_created AND first_bill_created + INTERVAL '2' MONTH
GROUP BY sb.act_acct_cd
)

,first_cycle_info AS (
SELECT  sb.act_acct_cd
        ,CASE 
            WHEN fi_sales_channel LIKE '%D2D%' THEN 'D2D'
            WHEN fi_sales_channel LIKE '%Digital%' THEN 'Digital'
            ELSE fi_sales_channel
        END As sales_channel
        ,MAX(pd_mix_qty) AS max_rgu /*Estamos relacionando el MRC más alto en sus primeros 3 meses. En ese caso, hace sentido traer el máximo valor de RGUs que haya tenido el usuario*/
        ,MAX(fi_rgu_vendidos) AS rgu_vendidos
        ,MIN(first_installation_date) AS first_installation_date
        ,MIN(sb.first_dna_date) AS first_dna_date
        ,MIN(first_bill_created) AS first_bill_created
        ,TRY(ARRAY_AGG(ARREGLO_MRC)[1]) AS ARREGLO_MRC
        -- ,MAX(fi_outst_age) AS max_outst_age_first_bill
        ,MAX(max_tot_mrc) AS max_tot_mrc
        -- ,COUNT(DISTINCT max_tot_mrc) AS DIFF_MRC
FROM sales_base AS sb
INNER JOIN mrc_calculation AS mrcc ON mrcc.act_acct_cd = sb.act_acct_cd
WHERE DATE(sb.fi_bill_dt_m0) = mrcc.first_bill_created
GROUP BY sb.act_acct_cd,fi_sales_channel
) 

,Payments_basic AS (
SELECT account_id AS act_acct_cd
        -- ,TRY(ARRAY_AGG(DATE(dt) ORDER BY DATE(dt))[1]) AS first_payment_date
        -- ,TRY(ARRAY_AGG(CAST(payment_amt_usd AS DOUBLE) ORDER BY DATE(dt))[1]) AS first_payment_amt
        -- ,TRY(FILTER(ARRAY_AGG(CASE WHEN CAST(payment_amt_usd AS DOUBLE) >= max_mrc_first_bill THEN DATE(DT) ELSE NULL END ORDER BY DATE(DT)), x -> x IS NOT NULL)[1]) AS first_payment_above_MRC_date
        -- ,TRY(FILTER(ARRAY_AGG(CASE WHEN CAST(payment_amt_usd AS DOUBLE) >= max_mrc_first_bill THEN CAST(payment_amt_usd AS DOUBLE) ELSE NULL END ORDER BY DATE(DT)), x -> x IS NOT NULL) [1]) AS first_payment_above_MRC
        ,TRY(ARRAY_AGG(DATE(DT) ORDER BY DATE(DT))[1]) AS FIRST_PAY_DATE
        ,TRY(ARRAY_AGG(DATE(DT) ORDER BY DATE(DT) DESC)[1]) AS LAST_PAY_DATE
        ,ARRAY_AGG(DATE(DT) ORDER BY DATE(DT)) AS ARREGLO_PAGOS_DATES
        ,ARRAY_AGG(CAST(payment_amt_usd AS DOUBLE) ORDER BY DATE(DT)) AS ARREGLO_PAGOS
        ,ROUND(SUM(CAST(payment_amt_usd AS DOUBLE)),2) AS total_payments_in_3_months
        ,ROUND(SUM(IF(DATE_DIFF('month', DATE(FC.first_bill_created), DATE(dt)) < 1 OR (EXTRACT(DAY FROM FC.first_bill_created) = EXTRACT(DAY FROM DATE(dt)) AND EXTRACT(MONTH FROM FC.first_bill_created) + 1 = EXTRACT(MONTH FROM DATE(dt))) ,CAST(payment_amt_usd AS DOUBLE), NULL)),2) AS total_payments_30_days
        ,ROUND(SUM(IF(DATE_DIFF('month', DATE(FC.first_bill_created), DATE(dt)) < 2 OR (EXTRACT(DAY FROM FC.first_bill_created) = EXTRACT(DAY FROM DATE(dt)) AND EXTRACT(MONTH FROM FC.first_bill_created) + 2 = EXTRACT(MONTH FROM DATE(dt))),CAST(payment_amt_usd AS DOUBLE), NULL)),2) AS total_payments_60_days
        ,ROUND(SUM(IF(DATE_DIFF('month', DATE(FC.first_bill_created), DATE(dt)) < 3 OR (EXTRACT(DAY FROM FC.first_bill_created) = EXTRACT(DAY FROM DATE(dt)) AND EXTRACT(MONTH FROM FC.first_bill_created) + 3 = EXTRACT(MONTH FROM DATE(dt))),CAST(payment_amt_usd AS DOUBLE), NULL)),2) AS total_payments_90_days
FROM "db-stage-prod"."payments_cwp"  AS P
INNER JOIN first_cycle_info AS FC ON FC.act_acct_cd = P.account_id
/*LOS DEPOSITOS PUEDEN SER PAGOS QUE SE HACEN ANTES DE LA EMISIÓN DE LA PRIMERA FACTURA*/
-- WHERE DATE(dt) BETWEEN FC.first_bill_created - INTERVAL '45' DAY AND FC.first_bill_created + INTERVAL '3' MONTH
-- Desde emisión de la primera factura
WHERE DATE(dt) BETWEEN FC.first_bill_created - interval '45' day AND FC.first_bill_created + INTERVAL '3' MONTH
-- Desde la instalación hasta 3 meses después de la emisión de la primera factura
-- WHERE DATE(dt) BETWEEN FC.first_installation_date AND FC.first_bill_created + INTERVAL '3' MONTH
-- Desde el first_dna_date hasta 3 meses después de la emisión de la primera factura
-- WHERE DATE(dt) BETWEEN FC.first_dna_date AND FC.first_bill_created + INTERVAL '3' MONTH

GROUP BY account_id
)

,gross_add_presummary AS (
SELECT (select input_month from parameters) as month_b,* 
    ,CASE   WHEN total_payments_30_days IS NULL THEN act_acct_cd
            WHEN total_payments_30_days < max_tot_mrc THEN act_acct_cd ELSE NULL END AS npn_30_flag
    ,CASE   WHEN total_payments_60_days IS NULL THEN act_acct_cd
            WHEN total_payments_60_days < max_tot_mrc THEN act_acct_cd ELSE NULL END AS npn_60_flag
    ,CASE   WHEN total_payments_90_days IS NULL THEN act_acct_cd
            WHEN total_payments_90_days < max_tot_mrc THEN act_acct_cd ELSE NULL END AS npn_90_flag
    ,CASE   WHEN total_payments_in_3_months IS NULL THEN act_acct_cd
            WHEN total_payments_in_3_months < max_tot_mrc THEN act_acct_cd ELSE NULL END AS npn_flag
    , DATE_DIFF('DAY', first_bill_created, CURRENT_DATE - INTERVAL '2' DAY)  AS days_to_npn
FROM first_cycle_info 
LEFT JOIN Payments_basic USING (act_acct_cd)
)

,summary_by_user AS (
SELECT * 
    ,if(days_to_npn > 90, 'Completed 90 Days', 'Uncomplete 90 days') AS Days_flag_90
    ,if(days_to_npn > 60, 'Completed 60 Days', 'Uncomplete 60 days') AS Days_flag_60
    ,if(days_to_npn > 30, 'Completed 30 Days', 'Uncomplete 30 days') AS Days_flag_30
            
FROM gross_add_presummary
INNER JOIN sales_channel_calculation USING(act_acct_cd)
)



--==================================== Long Installs ===========================================
,service_orders AS (
SELECT DISTINCT  *
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
        ,CASE WHEN B.installation_lapse > 5 THEN act_acct_cd ELSE null END AS late_inst_flag
FROM summary_by_user A LEFT JOIN service_orders B on A.month_b = B.month AND CAST(A.act_acct_cd AS VARCHAR) = CAST(B.account_id AS VARCHAR)
)

--==============================================================================================

------------------------------------- INTERACTIONS --------------------------------------

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


--======================================= Early Tech tickets ===================================

,early_ticket_info AS (
SELECT  F.account_id
        ,F.month
        ,CASE WHEN F.account_id IS NOT NULL THEN account_id ELSE NULL END AS early_ticket_flag
FROM sales_base E INNER JOIN interactions F ON E.act_acct_cd = F.account_id
WHERE DATE_DIFF('week',CAST(act_acct_inst_dt AS DATE),CAST(interaction_date AS DATE)) <= 7
    AND interaction_purpose_descrip = 'TICKET'
GROUP BY account_id, month
)

,early_tkt_flag AS (
SELECT  F.*
        ,early_ticket_flag
FROM late_installation_flag F LEFT JOIN early_ticket_info A ON F.act_acct_cd = A.account_id AND F.month_b = A.month
)



,DUPLICADOS AS (
select *, ROW_NUMBER()OVER(PARTITION BY ACT_ACCT_CD) AS ROW_COUNT  from early_tkt_flag
)


,final_decoupled as (
select * from duplicados where row_count = 1
)

-- select count(distinct act_acct_cd) from final_decoupled

,fmc_table AS (
SELECT * FROM "lla_cco_int_ana_dev"."cwp_fmc_churn_dev"
WHERE month = DATE(dt) AND month = (SELECT input_month FROM parameters) and ((Fixedchurntype != 'Fixed Voluntary Churner' AND Fixedchurntype != 'Fixed Involuntary Churner') OR Fixedchurntype IS NULL) AND finalchurnflag !='Fixed Churner' 
)

,sales_kpi_join as (
select f.*,s.* from fmc_table f full outer join final_decoupled s on act_acct_cd = fixedaccount and s.month_b = f.month
)

--======================================= Early Interaction ====================================

,new_customer_interactions_info AS (
SELECT  A.act_acct_cd
        ,B.account_id
        ,A.month_load
        ,CASE WHEN account_id IS NOT NULL THEN act_acct_cd ELSE null END AS early_interaction_flag
FROM sales_base A LEFT JOIN interactions B ON A.act_acct_cd = B.account_id
WHERE DATE_DIFF('DAY',CAST(act_acct_inst_dt AS DATE),CAST(interaction_date AS DATE)) <= 21
    AND interaction_type = 'Technical'
GROUP BY 1,2,3
)

,early_int_flag AS (
SELECT  F.*
        ,early_interaction_flag
FROM sales_kpi_join F LEFT JOIN new_customer_interactions_info A ON F.fixedaccount = A.act_acct_cd AND F.month_b = A.month_load
)


------------------------ Billing CLaims C6 -------------------------------
,stock_key AS (
SELECT  * 
        ,CONCAT(finalaccount,SUBSTR(CAST(month AS VARCHAR),1,7)) AS key_stock
FROM early_int_flag 
)

,customers_billing_claims AS (
SELECT  CONCAT(account_id, SUBSTR(CAST(DATE_TRUNC('Month',interaction_date) AS VARCHAR),1,7)) AS key_bill_claims
FROM interactions
WHERE interaction_type = 'Billing'
)

,billing_claims_flag AS (
SELECT  A.*
        ,CASE WHEN key_bill_claims IS NOT NULL THEN fixedaccount ELSE null END AS bill_claim_flag
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
        ,CASE WHEN mrc_change > 0.05 OR mrc_change < -0.05 THEN fixedaccount ELSE null END AS mrc_change_flag
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

,FullTable_KPIsFlags AS (
SELECT  *
        --,CASE WHEN bill_claim_flag = 1 THEN CONCAT(CAST(bill_claim_flag AS VARCHAR), fixedaccount) ELSE NULL END AS F_BillClaim
        --,CASE WHEN mrc_change_flag = 1 THEN CONCAT(CAST(mrc_change_flag AS VARCHAR), fixedaccount) ELSE NULL END AS F_MRCChange
        ,CASE WHEN Mounting_bill_flag = 1 THEN CONCAT(CAST(mounting_bill_flag AS VARCHAR), fixedaccount) ELSE NULL END AS F_MountingBillFlag
FROM mounting_bills_flag
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

--select count(distinct act_acct_cd) from FullTable_Adj
SELECT  
case when month is null then month_b else month end  as month
--/*
        ,B_Final_TechFlag
        ,B_FMCSegment
        ,E_Final_TechFlag
        ,E_FMCSegment
        ,b_final_tenure
        ,e_final_tenure
        ,B_FixedTenure
        ,E_FixedTenure
        ,Days_flag_30
        ,Days_flag_60
        ,Days_flag_90
-- */
        ,COUNT(DISTINCT fixedaccount) AS activebase
--/*
        -- ,SUM(monthsale_flag) AS Sales
        --,SUM(straight_soft_dx_flag) AS Soft_Dx
        -- ,null AS Soft_Dx
        -- ,SUM(NEVER_PAID_30_FLAG) AS NeverPaid_30
        -- ,SUM(NEVER_PAID_60_FLAG) AS NeverPaid_60
        -- ,SUM(NEVER_PAID_90_FLAG) AS NeverPaid_90
        -- ,SUM(late_inst_flag) AS Long_installs
        -- ,SUM(early_interaction_flag) AS Early_Issues
        -- ,SUM(early_ticket_flag) AS Early_ticket
--*/
        ,COUNT(DISTINCT act_acct_cd) AS Unique_Sales
        ,COUNT(DISTINCT npn_30_flag) AS Unique_NeverPaid_30
        ,COUNT(DISTINCT npn_60_flag) AS Unique_NeverPaid_60
        ,COUNT(DISTINCT npn_90_flag) AS Unique_NeverPaid_90
        ,COUNT(DISTINCT late_inst_flag) as Unique_LongInstall
        ,COUNT(DISTINCT early_interaction_flag) AS Unique_EarlyInteraction
        ,COUNT(DISTINCT early_ticket_flag) as Unique_EarlyTicket
        ,COUNT(DISTINCT bill_claim_flag) AS Unique_BillClaim
        ,COUNT(DISTINCT no_plan_change) AS NoPlan
        ,COUNT(DISTINCT mrc_change_flag) AS Unique_MRCChange
        ,COUNT(DISTINCT F_MountingBillFlag) AS Unique_MountingBill
-- /*
        ,B_FMCTYPE
        ,E_FMCTYPE
        ,first_sales_chnl_eom
        ,first_sales_chnl_bom
        ,Last_Sales_CHNL_EOM
        ,Last_Sales_CHNL_BOM 
        ,sales_channel
        ,sales_channel_so
--  */
-- SELECT *
FROM FullTable_Adj
--WHERE ((Fixedchurntype != 'Fixed Voluntary Churner' AND Fixedchurntype != 'Fixed Involuntary Churner') OR Fixedchurntype IS NULL) AND finalchurnflag !='Fixed Churner'  
GROUP BY 1,2,3,4,5,6,7,8,9,B_FMCTYPE, E_FMCTYPE ,Days_flag_30,Days_flag_60,Days_flag_90, first_sales_chnl_eom, first_sales_chnl_bom, Last_Sales_CHNL_EOM, Last_Sales_CHNL_BOM , sales_channel,sales_channel_so
ORDER BY 1

-- , summary as (
-- SELECT  (SELECT input_month FROM Parameters) as Month

--         ,COUNT(DISTINCT act_acct_cd) AS sales_base

--         ,COUNT(DISTINCT npn_30_flag) AS NPN_by_sum_payments_in_30_days

--         ,Days_flag_30
--         ,COUNT(DISTINCT npn_60_flag) AS NPN_by_sum_payments_in_60_days

--         ,Days_flag_60
--         ,COUNT(DISTINCT npn_90_flag) AS NPN_by_sum_payments_in_90_days

--         ,Days_flag_90

--         ,sum(late_inst_flag) as Long_installs
--         ,sum(early_ticket_flag) as Early_tickets
-- FROM DUPLICADOS WHERE ROW_COUNT = 1
-- GROUP BY 
-- Days_flag_30,Days_flag_60,Days_flag_90--,sales_channel
-- )

-- select * from summary 




