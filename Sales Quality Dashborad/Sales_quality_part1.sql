CREATE TABLE IF NOT EXISTS "lla_cco_int_san"."cwp_sales_quality_part1"  AS 

WITH 
Parameters AS (
SELECT 
DATE('2022-06-01') AS input_month,
date('2022-03-01') as current_month
 )
--###################### INFORMACIÓN GENERAL ####################################

/* 
Input: Gross Adds Table
Logic:Extraer de la tabla de gross adds la infromacion de cada una de los usuarios, cuenta, canal, canal_sub, codigo del vendedor, nombre del vendedor, rgus vendidos
*/
,sales_channel_calculation AS (
/*Es importante dejar clara la regla de atribución de sales channel, si un usuario tiene diferentes canales de ventas en el mes de consulta se atribuirá la venta al primero de estos*/
SELECT  sales_month,CAST(cuenta AS VARCHAR) AS act_acct_cd 
        ,segmento_nuevo as socioeconomic_seg
        ,TRY(ARRAY_AGG(sales_channel ORDER BY DATE(sales_month)) [1]) as fi_sales_channel
        ,TRY(ARRAY_AGG(sales_channel_sub ORDER BY DATE(sales_month)) [1]) as fi_sales_channel_sub
        ,TRY(ARRAY_AGG(codigo_de_vendedor ORDER BY DATE(sales_month)) [1]) as fi_codigo_de_vendedor
        ,TRY(ARRAY_AGG(nombre_vendedor ORDER BY DATE(sales_month)) [1]) as fi_nombre_vendedor
        ,TRY(ARRAY_AGG(rgus_vendidos ORDER BY DATE(sales_month)) [1]) as fi_rgu_vendidos
FROM "db-stage-prod"."cwp_gross_adds_fijo" 
WHERE sales_month != '' and sales_month IS NOT NULL
     AND DATE_TRUNC('MONTH', DATE(sales_month)) = (SELECT input_month FROM Parameters)
GROUP BY sales_month,cuenta,segmento_nuevo
)

/*
Input: DNA fixed 
Logica: Tomar los usuarios que no han estado presentes en el DNA en los utlimos 6 meses
*/
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
/*
Input: DNA Fixed
Logica: Sacar todos los campos necesrios del dna
*/
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
            /*
            ,pd_bb_accs_media
            ,pd_TV_accs_media
            ,pd_VO_accs_media
            */
            ,Case   When pd_bb_accs_media = 'FTTH' Then 'FTTH'
                When pd_bb_accs_media = 'HFC' Then 'HFC'
                when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then 'FTTH'
                when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then 'HFC'
                when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'FTTH'
                when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'HFC'
                ELSE 'COPPER'
                    end as TechFlag
                    
            , Case when act_prvnc_cd = '1' then 'Bocas del Toro'
                when act_prvnc_cd = '2' then 'Cocle'
                when act_prvnc_cd = '3' then 'Colon'
                when act_prvnc_cd = '4' then 'Chiriqui'
                when act_prvnc_cd = '5' then 'Darien'
                when act_prvnc_cd = '6' then 'Herrera'
                when act_prvnc_cd = '7' then 'Los Santos'
                when act_prvnc_cd = '8' then 'Panama'
                when act_prvnc_cd = '9' then 'Veraguas'
                when act_prvnc_cd = '10' then 'Panama Oeste'
                else null end as geography
            --,act_acct_inst_dt
            --,act_cust_strt_dt
            ,act_cust_typ_nm
            --,DATE_TRUNC('month',DATE(dt)) AS month_load
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
    WHERE DATE(dt) between  (SELECT input_month FROM Parameters) AND  (SELECT input_month FROM Parameters) + INTERVAL '4' MONTH 
)
/*
Input: Sales_channel_calculation y dna_usful_fields 
Logica: Cruzar las tablas de gross adds y el dna para agregar toda la infromación de los usuarios identificados en la tabla de gross adds
*/
,main_info_sales_base AS (
SELECT *
FROM sales_channel_calculation AS b
INNER JOIN dna_usefull_fields AS a USING(act_acct_cd) 
)


--############# GROSS ADDS #################
/*
Input: main_usfull_fields y sales_candidates 
Logica: Cruzar las tablas de usuarios identificados en la tabla de gross adds con los candidatos de ventas, estos son los Gross Adds
*/
,sales_base AS (
SELECT *
FROM main_info_sales_base AS a
INNER JOIN candidates_sales AS b USING(act_acct_cd)
) 
/*
Input: Sales Base
Logica: Agregar la fecha de la primera factura de los usuarios identificados como gross adds
*/
,bills_of_interest AS (
SELECT act_acct_cd,
    /*Usamos la fecha de la primera factura generada y no del oldes_unpaid_bill para no ser susceptibles a errores en el fi_outst_age o oldet_unpaid_bill*/
    DATE(TRY(FILTER(ARRAY_AGG(fi_bill_dt_m0 ORDER BY DATE(dt)), x -> x IS NOT NULL)[1])) AS first_bill_created
FROM sales_base
GROUP BY act_acct_cd 
)

/*
Input: Sales Base y bills_of_interest
Logica: Encontrar la primera factura creada en los primeros tres meses del usuario nuevo , el máximo fi_tot_mrc_amt en los primeros tres meses y el conjunto de mrc que se identiifcan en esos tres primeros meses 
*/
,mrc_calculation AS (
SELECT sb.act_acct_cd,
        MIN(bi.first_bill_created) AS first_bill_created, 
        MAX(fi_tot_mrc_amt) AS max_tot_mrc 
        --ARRAY_AGG(DISTINCT fi_tot_mrc_amt ORDER BY fi_tot_mrc_amt DESC) AS ARREGLO_MRC
FROM sales_base AS sb
INNER JOIN bills_of_interest AS bi ON sb.act_acct_cd = bi.act_acct_cd AND sb.dt BETWEEN first_bill_created AND first_bill_created + INTERVAL '2' MONTH
GROUP BY sb.act_acct_cd
)

/*
Input: sales_base y mcr_calculation
Logica: agregar las columnas de firstbill created y regus vendidos (gross table) y rgus presentes en el dna
*/
,first_cycle_info AS (
SELECT  sb.act_acct_cd
        ,MAX(pd_mix_qty) AS max_rgu /*Estamos relacionando el MRC más alto en sus primeros 3 meses. En ese caso, hace sentido traer el máximo valor de RGUs que haya tenido el usuario*/
        ,MAX(fi_rgu_vendidos) AS rgu_vendidos
        --,MIN(first_installation_date) AS first_installation_date
        ,MIN(first_bill_created) AS first_bill_created
        -- ,TRY(ARRAY_AGG(ARREGLO_MRC)[1]) AS ARREGLO_MRC
        -- ,MAX(fi_outst_age) AS max_outst_age_first_bill
        ,MAX(max_tot_mrc) AS max_tot_mrc
        -- ,COUNT(DISTINCT max_tot_mrc) AS DIFF_MRC
FROM sales_base AS sb
-- INNER JOIN bills_of_interest AS fb ON fb.act_acct_cd = sb.act_acct_cd AND fb.first_bill_created = DATE(sb.fi_bill_dt_m0)
INNER JOIN mrc_calculation AS mrcc ON mrcc.act_acct_cd = sb.act_acct_cd
WHERE DATE(sb.fi_bill_dt_m0) = mrcc.first_bill_created
GROUP BY sb.act_acct_cd
) 

/* 
Input: sales_base y base de pagos
Logica: agregar a la sales base la primera y ultima fecha de pago, el total de pagos realizados a 30, 60 y 90 dias - solo se tienen en cuenta los usuarios
considerados parte del sale base 
*/
,Payments_basic AS (
SELECT account_id AS act_acct_cd
        --,FC.first_bill_created
        -- ,TRY(ARRAY_AGG(DATE(dt) ORDER BY DATE(dt))[1]) AS first_payment_date
        -- ,TRY(ARRAY_AGG(CAST(payment_amt_usd AS DOUBLE) ORDER BY DATE(dt))[1]) AS first_payment_amt
        -- ,TRY(FILTER(ARRAY_AGG(CASE WHEN CAST(payment_amt_usd AS DOUBLE) >= max_mrc_first_bill THEN DATE(DT) ELSE NULL END ORDER BY DATE(DT)), x -> x IS NOT NULL)[1]) AS first_payment_above_MRC_date
        -- ,TRY(FILTER(ARRAY_AGG(CASE WHEN CAST(payment_amt_usd AS DOUBLE) >= max_mrc_first_bill THEN CAST(payment_amt_usd AS DOUBLE) ELSE NULL END ORDER BY DATE(DT)), x -> x IS NOT NULL) [1]) AS first_payment_above_MRC
        -- ,TRY(ARRAY_AGG(DATE(DT) ORDER BY DATE(DT))[1]) AS FIRST_PAY_DATE
        -- ,TRY(ARRAY_AGG(DATE(DT) ORDER BY DATE(DT) DESC)[1]) AS LAST_PAY_DATE
        -- ,ARRAY_AGG(DATE(DT) ORDER BY DATE(DT)) AS ARREGLO_PAGOS_DATES
        -- ,ARRAY_AGG(CAST(payment_amt_usd AS DOUBLE) ORDER BY DATE(DT)) AS ARREGLO_PAGOS
        ,ROUND(SUM(CAST(payment_amt_usd AS DOUBLE)),2) AS total_payments_in_3_months
        ,ROUND(SUM(IF(DATE_DIFF('month', DATE(FC.first_bill_created), DATE(dt)) < 1 OR (EXTRACT(DAY FROM FC.first_bill_created) = EXTRACT(DAY FROM DATE(dt)) AND EXTRACT(MONTH FROM FC.first_bill_created) + 1 = EXTRACT(MONTH FROM DATE(dt))) ,CAST(payment_amt_usd AS DOUBLE), NULL)),2) AS total_payments_30_days
        ,ROUND(SUM(IF(DATE_DIFF('month', DATE(FC.first_bill_created), DATE(dt)) < 2 OR (EXTRACT(DAY FROM FC.first_bill_created) = EXTRACT(DAY FROM DATE(dt)) AND EXTRACT(MONTH FROM FC.first_bill_created) + 2 = EXTRACT(MONTH FROM DATE(dt))),CAST(payment_amt_usd AS DOUBLE), NULL)),2) AS total_payments_60_days
        ,ROUND(SUM(IF(DATE_DIFF('month', DATE(FC.first_bill_created), DATE(dt)) < 3 OR (EXTRACT(DAY FROM FC.first_bill_created) = EXTRACT(DAY FROM DATE(dt)) AND EXTRACT(MONTH FROM FC.first_bill_created) + 3 = EXTRACT(MONTH FROM DATE(dt))),CAST(payment_amt_usd AS DOUBLE), NULL)),2) AS total_payments_90_days
FROM "db-stage-prod"."payments_cwp"  AS P
INNER JOIN first_cycle_info AS FC ON FC.act_acct_cd = P.account_id
/*LOS DEPOSITOS PUEDEN SER PAGOS QUE SE HACEN ANTES DE LA EMISIÓN DE LA PRIMERA FACTURA*/
WHERE DATE(dt) BETWEEN FC.first_bill_created - INTERVAL '45' DAY AND FC.first_bill_created + INTERVAL '3' MONTH
GROUP BY account_id
)
--select * from payments_basic where total_payments_in_3_months <> total_payments_90_days
/*
Input: payments y first_bill_cycle
Logica: Agregar las flags de indentificar si es un never paid o no a 30 60 y 90 dias
*/
,gross_add_presummary AS (
SELECT * 
    ,CASE   WHEN total_payments_30_days IS NULL THEN act_acct_cd
            WHEN total_payments_30_days < max_tot_mrc THEN act_acct_cd ELSE NULL END AS npn_30_flag
    ,CASE   WHEN total_payments_60_days IS NULL THEN act_acct_cd
            WHEN total_payments_60_days < max_tot_mrc THEN act_acct_cd ELSE NULL END AS npn_60_flag
    ,CASE   WHEN total_payments_90_days IS NULL THEN act_acct_cd
            WHEN total_payments_90_days < max_tot_mrc THEN act_acct_cd ELSE NULL END AS npn_90_flag
    ,CASE   WHEN total_payments_in_3_months IS NULL THEN act_acct_cd
            WHEN total_payments_in_3_months < max_tot_mrc THEN act_acct_cd ELSE NULL END AS npn_flag
FROM first_cycle_info 
LEFT JOIN Payments_basic USING (act_acct_cd)
)

--###################### INFORMACIÓN PARA NPN DE CROSS SELLS ####################################
/*
Input: Main_sale_base (usuarios que aparecen en la base de gross adds junto con su infromacion del dna) y candidates (usuariosidentificados como gross adds 
Logica:Sacar la main sale base todos los usuarios que no han sido clasificados como gross adds, encontrar el mix al inicio y a final de mes al igual que el 
mrc a inicio y a final de mes. Sacar los rgus vendidos del archivo de gross adds y la diferencia de rgus en el DNA 
*/

,upsell_base AS (
/*Encontramos a todos los usuarios en el mes de interés que tuvieron un incremento en RGUs entre el último y el primer día del mes*/
SELECT   act_acct_cd
        ,if(last_pd_mix_qty > first_pd_mix_qty, 'Upsell','Other') AS upsell_type
        ,max(first_pd_mix_qty) as first_pd_mix_qty
        ,max(first_rgu_qty) as first_rgu_qty
        ,MAX(last_pd_mix_qty) AS last_pd_mix_qty
        ,MAX(last_rgu_qty) AS last_rgu_qty
        /*Variacion de RGUs estimada a partir de los nombres de los producutos - considera el cambio de productos como una venta de rgu*/
        ,SUM( 
            IF(last_pd[1] != first_pd[1], 1, 0) +
            IF(last_pd[2] != first_pd[2], 1, 0) +
            IF(last_pd[3] != first_pd[3], 1, 0)
            ) AS upsell_rgu_with_planchanges
        /*Variacion de RGUs estimada a partir del mix de los usuarios (0P,1P,2P,3P)*/
        ,SUM(last_pd_mix_qty-first_pd_mix_qty) as upsell_rgus
        /*Variacion de RGUs estimada a partir de los nombres de los productos - no considera el cambio de productos como una venta de rgu*/
        ,SUM(last_rgu_qty - first_rgu_qty) as upsell_rgus_names
        ,MAX(TRY(FILTER(pd_mix_array_date, x -> CAST(x[1] AS INTEGER) = last_pd_mix_qty)[1])) AS array_change_rgu
        --,ROUND(MAX(last_mrc - first_mrc),2) AS upsell_mrc
        --,MAX(last_mrc) AS  last_mrc
        --,MAX(IF(last_pd[1] != first_pd[1] OR last_pd[2] != first_pd[2] OR last_pd[3] != first_pd[3], TRUE, FALSE)) AS var_pd_nm
        --,MAX(FIRST_PD) AS FIRST_PD
        --,MAX(last_pd) AS LAST_PD
FROM (
    SELECT act_acct_cd
             /*Primera cantidad de rgus basado en la columna del mis del usuario (0P,1P,2P,3P)*/
            ,TRY(ARRAY_AGG(pd_mix_qty ORDER BY dt)[1]) AS first_pd_mix_qty
            /*Primera cantidad de rgus basado en los nombres de los productos*/
            ,(CASE WHEN TRY(ARRAY_AGG(pd_bb_prod_nm ORDER BY dt)[1]) IS NOT NULL THEN 1 ELSE 0 END ) + 
             (CASE WHEN TRY(ARRAY_AGG(pd_tv_prod_nm ORDER BY dt)[1]) IS NOT NULL THEN 1 ELSE 0 END) + 
             (CASE WHEN TRY(ARRAY_AGG(pd_vo_prod_nm ORDER BY dt)[1]) IS NOT NULL THEN 1 ELSE 0 END) AS first_rgu_qty
             /*Ultima cantidad de rgus basado en la columna del mis del usuario (0P,1P,2P,3P)*/
            ,TRY(ARRAY_AGG(pd_mix_qty ORDER BY dt DESC)[1]) AS last_pd_mix_qty
            /*Ultima cantidad de rgus basado en los nombres de los productos*/
            ,(CASE WHEN TRY(ARRAY_AGG(pd_bb_prod_nm ORDER BY dt DESC)[1]) IS NOT NULL THEN 1 ELSE 0 END) + 
             (CASE WHEN TRY(ARRAY_AGG(pd_tv_prod_nm ORDER BY dt DESC)[1]) IS NOT NULL THEN 1 ELSE 0 END) +
             (CASE WHEN TRY(ARRAY_AGG(pd_vo_prod_nm ORDER BY dt DESC)[1]) IS NOT NULL THEN 1 ELSE 0 END) AS last_rgu_qty
            --,TRY(ARRAY_AGG(CAST(fi_tot_mrc_amt AS DOUBLE) ORDER BY dt)[1]) AS first_mrc
            --,TRY(ARRAY_AGG(CAST(fi_tot_mrc_amt AS DOUBLE) ORDER BY dt DESC)[1]) AS last_mrc
            ,ARRAY_AGG(ARRAY[CAST(pd_mix_qty AS VARCHAR) , CAST(dt AS VARCHAR)] ORDER BY dt) AS pd_mix_array_date
            -- ,ARRAY_AGG(ARRAY[CAST(fi_tot_mrc_amt AS VARCHAR) , CAST(dt AS VARCHAR)] ORDER BY dt) AS mrc_array_date
            -- ,ARRAY_AGG(ARRAY[pd_mix_nm, pd_bb_prod_nm, pd_vo_prod_nm, pd_tv_prod_nm,CAST(fi_tot_mrc_amt AS VARCHAR),CAST(dt AS VARCHAR)] ORDER BY dt) AS pd_array_date
            ,TRY(ARRAY_AGG(ARRAY[COALESCE(pd_bb_prod_nm, 'NA'), COALESCE(pd_vo_prod_nm, 'NA'), COALESCE(pd_tv_prod_nm, 'NA')] ORDER BY dt)[1]) AS first_pd
            ,TRY(ARRAY_AGG(ARRAY[COALESCE(pd_bb_prod_nm, 'NA'), COALESCE(pd_vo_prod_nm, 'NA'), COALESCE(pd_tv_prod_nm, 'NA')] ORDER BY dt DESC)[1]) AS last_pd
            
    FROM main_info_sales_base
    WHERE DATE_TRUNC('MONTH', dt) = (SELECT input_month FROM Parameters)
        AND act_acct_cd NOT IN (SELECT act_acct_cd FROM candidates_sales)
    GROUP BY act_acct_cd
    )
    -- WHERE ((last_pd_mix_qty > first_pd_mix_qty AND first_pd_mix_qty>0 AND first_mrc>0 AND first_pd_mix_qty IS NOT NULL)  OR (last_pd_mix_qty = first_pd_mix_qty AND last_mrc > first_mrc AND first_mrc>0 )) AND last_pd != first_pd
    -- AND act_acct_cd NOT IN (SELECT act_acct_cd FROM sales_base)
    GROUP BY act_acct_cd, 2
)


,bills_of_interest_upsell_base AS (
SELECT b.act_acct_cd
        ,max(first_pd_mix_qty) as first_pd_mix_qty
        ,max(first_rgu_qty) as first_rgu_qty
        ,MAX(last_pd_mix_qty) AS last_pd_mix_qty
        ,MAX(last_rgu_qty) AS last_rgu_qty
        ,MAX(upsell_type) AS upsell_type
        ,MAX(upsell_rgus) AS upsell_rgus
        ,MAX(upsell_rgus_names) AS upsell_rgus_names
        ,MAX(upsell_rgu_with_planchanges) AS upsell_rgu_with_planchanges
        --,MAX(rgu_vendidos) AS rgu_vendidos
        ,MAX(max_rgu) AS max_rgu
        ,MIN(upsell_update_date) AS upsell_update_date
        ,MIN(DATE(fi_bill_dt_m0)) AS first_bill_after_upsell
        --,max(sale_rep) as sale_rep
        --,max(socioeconomic_seg) as socioeconomic_seg
FROM dna_usefull_fields AS a
INNER JOIN (
    SELECT act_acct_cd
            ,max(first_pd_mix_qty) as first_pd_mix_qty
            ,max(first_rgu_qty) as first_rgu_qty
            ,MAX(last_pd_mix_qty) AS last_pd_mix_qty
            ,MAX(last_rgu_qty) AS last_rgu_qty
            ,MAX(upsell_type) AS upsell_type
            ,MAX(upsell_rgus) AS upsell_rgus
            ,MAX(upsell_rgu_with_planchanges) AS upsell_rgu_with_planchanges
            ,MAX(upsell_rgus_names) AS upsell_rgus_names
            --,MAX(rgu_vendidos) AS rgu_vendidos
            ,MAX(last_pd_mix_qty) AS max_rgu
            ,MIN(array_change_rgu[2]) AS upsell_update_date
            --,max(sale_rep) as sale_rep
            --,max(socioeconomic_seg) as socioeconomic_seg
    FROM upsell_base
    GROUP BY act_acct_cd
    ) AS b ON a.act_acct_cd = b.act_acct_cd AND  DATE(a.fi_bill_dt_m0) >= DATE(upsell_update_date)
GROUP BY b.act_acct_cd
)


,MRC_upsell AS (
SELECT  a.act_acct_cd
        ,max(first_pd_mix_qty) as first_pd_mix_qty
        ,max(first_rgu_qty) as first_rgu_qty
        ,MAX(last_pd_mix_qty) AS last_pd_mix_qty
        ,MAX(last_rgu_qty) AS last_rgu_qty
        ,MAX(upsell_type) AS upsell_type
        ,MAX(b.upsell_rgus) AS upsell_rgus
        ,MAX(b.upsell_rgu_with_planchanges) AS upsell_rgu_with_planchanges
        ,MAX(upsell_rgus_names) AS upsell_rgus_names
        --,MAX(rgu_vendidos) AS rgu_vendidos
        ,MAX(b.max_rgu) AS max_rgu
        --,MIN(upsell_update_date) AS upsell_update_date
        ,MIN(first_bill_after_upsell) AS first_bill_after_upsell
        ,MAX(fi_tot_mrc_amt) AS max_tot_mrc
        --,ARRAY_AGG(DISTINCT fi_tot_mrc_amt ORDER BY fi_tot_mrc_amt DESC) AS ARREGLO_MRC
FROM dna_usefull_fields AS a
INNER JOIN bills_of_interest_upsell_base AS b ON a.act_acct_cd = b.act_acct_cd AND a.dt BETWEEN b.first_bill_after_upsell AND first_bill_after_upsell + INTERVAL '2' MONTH
GROUP BY a.act_acct_cd
)

,upsell_payments AS (
SELECT  act_acct_cd
        --,TRY(ARRAY_AGG(DATE(DT) ORDER BY DATE(DT))[1]) AS FIRST_PAY_DATE
        --,TRY(ARRAY_AGG(DATE(DT) ORDER BY DATE(DT) DESC)[1]) AS LAST_PAY_DATE
        --,ARRAY_AGG(DATE(DT) ORDER BY DATE(DT)) AS ARREGLO_PAGOS_DATES
        --,ARRAY_AGG(CAST(payment_amt_usd AS DOUBLE) ORDER BY DATE(DT)) AS ARREGLO_PAGOS
        ,ROUND(SUM(CAST(payment_amt_usd AS DOUBLE)),2) AS total_payments_in_3_months
        ,ROUND(SUM(IF(DATE_DIFF('month', DATE(b.first_bill_after_upsell), DATE(dt)) < 1 OR (EXTRACT(DAY FROM b.first_bill_after_upsell) = EXTRACT(DAY FROM DATE(dt)) AND EXTRACT(MONTH FROM b.first_bill_after_upsell) + 1 = EXTRACT(MONTH FROM DATE(dt))) ,CAST(payment_amt_usd AS DOUBLE), NULL)),2) AS total_payments_30_days
        ,ROUND(SUM(IF(DATE_DIFF('month', DATE(b.first_bill_after_upsell), DATE(dt)) < 2 OR (EXTRACT(DAY FROM b.first_bill_after_upsell) = EXTRACT(DAY FROM DATE(dt)) AND EXTRACT(MONTH FROM b.first_bill_after_upsell) + 2 = EXTRACT(MONTH FROM DATE(dt))),CAST(payment_amt_usd AS DOUBLE), NULL)),2) AS total_payments_60_days
        ,ROUND(SUM(IF(DATE_DIFF('month', DATE(b.first_bill_after_upsell), DATE(dt)) < 3 OR (EXTRACT(DAY FROM b.first_bill_after_upsell) = EXTRACT(DAY FROM DATE(dt)) AND EXTRACT(MONTH FROM b.first_bill_after_upsell) + 3 = EXTRACT(MONTH FROM DATE(dt))),CAST(payment_amt_usd AS DOUBLE), NULL)),2) AS total_payments_90_days
FROM "db-stage-prod"."payments_cwp"  AS P
INNER JOIN MRC_upsell AS b ON b.act_acct_cd = CAST(P.account_id AS VARCHAR) AND DATE(P.dt) BETWEEN first_bill_after_upsell AND first_bill_after_upsell + INTERVAL '3' MONTH
GROUP BY act_acct_cd, max_tot_mrc
)

,upsell_presummary AS (
SELECT *
        ,CASE   WHEN total_payments_30_days IS NULL THEN act_acct_cd
                WHEN total_payments_30_days < max_tot_mrc THEN act_acct_cd ELSE NULL END AS npn_30_flag
        ,CASE   WHEN total_payments_60_days IS NULL THEN act_acct_cd
                WHEN total_payments_60_days < max_tot_mrc THEN act_acct_cd ELSE NULL END AS npn_60_flag
        ,CASE   WHEN total_payments_90_days IS NULL THEN act_acct_cd
                WHEN total_payments_90_days < max_tot_mrc THEN act_acct_cd ELSE NULL END AS npn_90_flag
        ,CASE   WHEN total_payments_in_3_months IS NULL THEN act_acct_cd
                WHEN total_payments_in_3_months < max_tot_mrc THEN act_acct_cd ELSE NULL END AS npn_flag
FROM MRC_upsell AS a
LEFT JOIN upsell_payments USING (act_acct_cd)
)

--#### UNIMOS LAS TABLAS DE GROSS ADDS Y UPSELLS
,MERGING_BASES AS (
SELECT 'Upsell' AS base_classification, upsell_type,act_acct_cd,first_pd_mix_qty,first_rgu_qty,last_pd_mix_qty,last_rgu_qty,max_rgu as last_rgu, upsell_rgus,upsell_rgu_with_planchanges,upsell_rgus_names,
        --upsell_update_date, first_bill_after_upsell,NULL AS first_installation_date,
        NULL AS first_bill_created, max_tot_mrc, 
        --ARREGLO_MRC, FIRST_PAY_DATE, LAST_PAY_DATE, ARREGLO_PAGOS_DATES, ARREGLO_PAGOS, 
        total_payments_in_3_months, total_payments_30_days, total_payments_60_days, total_payments_90_days
        ,npn_30_flag, npn_60_flag, npn_90_flag, npn_flag
FROM upsell_presummary AS u
UNION ALL
SELECT 'Gross-add' AS base_classification, NULL AS upsell_type, act_acct_cd, null as first_pd_mix_qty,null as first_rgu_qty,null as last_pd_mix_qty,null as last_rgu_qty,max_rgu as last_rgu,NULL AS upsell_rgus,null as upsell_rgu_with_planchanges,
        null as upsell_rgus_names,
        --NULL AS upsell_update_date,NULL AS first_bill_after_upsell,first_installation_date,
        first_bill_created,max_tot_mrc,
        --ARREGLO_MRC,FIRST_PAY_DATE,LAST_PAY_DATE,ARREGLO_PAGOS_DATES,ARREGLO_PAGOS,
        total_payments_in_3_months,total_payments_30_days,total_payments_60_days,total_payments_90_days,
        npn_30_flag,npn_60_flag,npn_90_flag,npn_flag
FROM gross_add_presummary AS g
)


,summary_by_user AS (
SELECT a.*, sales_month,socioeconomic_seg,fi_sales_channel, fi_sales_channel_sub, fi_codigo_de_vendedor, fi_nombre_vendedor, fi_rgu_vendidos,c.techflag,c.geography
FROM MERGING_BASES a
INNER JOIN sales_channel_calculation  b on a.act_acct_cd = b.act_acct_cd
INNER JOIN dna_usefull_fields c on a.act_acct_cd = c.act_acct_cd
)


,summary_flag as (
SELECT *, case 
when base_classification = 'Gross-add' then 'Gross Add'
when upsell_type = 'Upsell' and upsell_rgus_names >0 then 'Upsell'
else 'Other'  end as movement_flag
, case
when base_classification = 'Gross-add' and last_rgu = 1 then '1P'
when base_classification = 'Gross-add' and last_rgu = 2 then '2P'
when base_classification = 'Gross-add' and last_rgu = 3 then '3P'
when base_classification in ('Upsell', 'Other') and first_pd_mix_qty = 1 then '1P'
when base_classification in ('Upsell', 'Other') and first_pd_mix_qty = 2 then '2P'
when base_classification in ('Upsell', 'Other') and first_pd_mix_qty = 3 then '3P'
else null  end as bunddle_type
, case when base_classification = 'Gross-add' then 0 else first_pd_mix_qty end as first_rgu_cnt
, case when base_classification = 'Gross-add' then last_rgu else last_pd_mix_qty end as last_rgu_cnt
, case when base_classification = 'Gross-add' then last_rgu else last_pd_mix_qty end - case when base_classification = 'Gross-add' then 0 else first_pd_mix_qty end as rgus_sold
from summary_by_user 
)

,gross_adds_view as (
select distinct 
sales_month,act_acct_cd,techflag,bunddle_type,socioeconomic_seg,geography,first_rgu_cnt,last_rgu_cnt,rgus_sold, movement_flag,fi_sales_channel as sales_channel, case when fi_sales_channel = 'Contractors' then fi_sales_channel_sub else null end as contractor, fi_codigo_de_vendedor as cod_sales_rep,npn_30_flag,npn_60_flag,npn_90_flag,npn_flag
from summary_flag
)

select * from gross_adds_view
