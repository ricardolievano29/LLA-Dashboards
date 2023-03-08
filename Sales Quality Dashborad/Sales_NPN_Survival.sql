WITH 
Parameters AS (
SELECT 
DATE('2022-02-01') AS input_month
)
--###################### INFORMACIÓN GENERAL ####################################

/* 
Input: Gross Adds Table
Logic:Extraer de la tabla de gross adds la infromacion de cada una de los usuarios, cuenta, canal, canal_sub, codigo del vendedor, nombre del vendedor, rgus vendidos
*/
,sales_channel_calculation AS (
/*Es importante dejar clara la regla de atribución de sales channel, si un usuario tiene diferentes canales de ventas en el mes de consulta se atribuirá la venta al primero de estos*/
SELECT  sales_month,CAST(cuenta AS VARCHAR) AS act_acct_cd 
        ,TRY(ARRAY_AGG(sales_channel ORDER BY DATE(sales_month)) [1]) as fi_sales_channel
        ,TRY(ARRAY_AGG(sales_channel_sub ORDER BY DATE(sales_month)) [1]) as fi_sales_channel_sub
        ,TRY(ARRAY_AGG(codigo_de_vendedor ORDER BY DATE(sales_month)) [1]) as fi_codigo_de_vendedor
        ,TRY(ARRAY_AGG(nombre_vendedor ORDER BY DATE(sales_month)) [1]) as fi_nombre_vendedor
        ,TRY(ARRAY_AGG(rgus_vendidos ORDER BY DATE(sales_month)) [1]) as fi_rgu_vendidos
FROM "db-stage-prod"."cwp_gross_adds_fijo" 
WHERE sales_month != '' and sales_month IS NOT NULL
     AND DATE_TRUNC('MONTH', DATE(sales_month)) between (SELECT input_month FROM Parameters) and (SELECT end_month FROM Parameters)
GROUP BY sales_month,cuenta
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
            ,FIRST_VALUE(DATE(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY act_acct_inst_dt) AS first_installation_date
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
        MAX(TechFlag) as tech_flag,
        MAX(geography) as geography,
        MAX(fi_codigo_de_vendedor) as sales_rep,
        MIN(bi.first_bill_created) AS first_bill_created, 
        MAX(fi_tot_mrc_amt) AS max_tot_mrc, 
        ARRAY_AGG(DISTINCT fi_tot_mrc_amt ORDER BY fi_tot_mrc_amt DESC) AS ARREGLO_MRC
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
        ,MAX(tech_flag) as tech_flag
        ,max(sb.geography) as geography
        ,max(mrcc.sales_rep) as sale_rep
        ,MAX(pd_mix_qty) AS max_rgu /*Estamos relacionando el MRC más alto en sus primeros 3 meses. En ese caso, hace sentido traer el máximo valor de RGUs que haya tenido el usuario*/
        ,MAX(fi_rgu_vendidos) AS rgu_vendidos
        ,MIN(first_installation_date) AS first_installation_date
        ,MIN(first_bill_created) AS first_bill_created
        ,TRY(ARRAY_AGG(ARREGLO_MRC)[1]) AS ARREGLO_MRC
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
        ,MAX(tech_flag) as tech_flag
        ,max(geography) as geography
        ,max(sale_rep) as sale_rep
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
        ,MAX(rgu_vendidos) AS rgu_vendidos
        ,MAX(TRY(FILTER(pd_mix_array_date, x -> CAST(x[1] AS INTEGER) = last_pd_mix_qty)[1])) AS array_change_rgu
        ,ROUND(MAX(last_mrc - first_mrc),2) AS upsell_mrc
        ,MAX(last_mrc) AS  last_mrc
        ,MAX(IF(last_pd[1] != first_pd[1] OR last_pd[2] != first_pd[2] OR last_pd[3] != first_pd[3], TRUE, FALSE)) AS var_pd_nm
        ,MAX(FIRST_PD) AS FIRST_PD
        ,MAX(last_pd) AS LAST_PD
FROM (
    SELECT act_acct_cd
            ,MAX(techflag) as tech_flag
            ,max(geography) as geography
            ,MAX(fi_rgu_vendidos) AS rgu_vendidos
            ,max(fi_codigo_de_vendedor) as sale_rep
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
            ,TRY(ARRAY_AGG(CAST(fi_tot_mrc_amt AS DOUBLE) ORDER BY dt)[1]) AS first_mrc
            ,TRY(ARRAY_AGG(CAST(fi_tot_mrc_amt AS DOUBLE) ORDER BY dt DESC)[1]) AS last_mrc
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
    GROUP BY act_acct_cd, 5
)

/*

*/
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
        ,MAX(rgu_vendidos) AS rgu_vendidos
        ,MAX(max_rgu) AS max_rgu
        ,MIN(upsell_update_date) AS upsell_update_date
        ,MIN(DATE(fi_bill_dt_m0)) AS first_bill_after_upsell
        ,max(sale_rep) as sale_rep
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
            ,MAX(rgu_vendidos) AS rgu_vendidos
            ,MAX(last_pd_mix_qty) AS max_rgu
            ,MIN(array_change_rgu[2]) AS upsell_update_date
            ,max(sale_rep) as sale_rep
    FROM upsell_base
    GROUP BY act_acct_cd
    ) AS b ON a.act_acct_cd = b.act_acct_cd AND  DATE(a.fi_bill_dt_m0) >= DATE(upsell_update_date)
GROUP BY b.act_acct_cd
)

/*

*/
,MRC_upsell AS (
SELECT  a.act_acct_cd
        ,max(techflag) as tech_flag
        ,max(geography) as geography
        ,max(sale_rep) as sale_rep
        ,max(first_pd_mix_qty) as first_pd_mix_qty
        ,max(first_rgu_qty) as first_rgu_qty
        ,MAX(last_pd_mix_qty) AS last_pd_mix_qty
        ,MAX(last_rgu_qty) AS last_rgu_qty
        ,MAX(upsell_type) AS upsell_type
        ,MAX(b.upsell_rgus) AS upsell_rgus
        ,MAX(b.upsell_rgu_with_planchanges) AS upsell_rgu_with_planchanges
        ,MAX(upsell_rgus_names) AS upsell_rgus_names
        ,MAX(rgu_vendidos) AS rgu_vendidos
        ,MAX(b.max_rgu) AS max_rgu
        ,MIN(upsell_update_date) AS upsell_update_date
        ,MIN(first_bill_after_upsell) AS first_bill_after_upsell
        ,MAX(fi_tot_mrc_amt) AS max_tot_mrc
        ,ARRAY_AGG(DISTINCT fi_tot_mrc_amt ORDER BY fi_tot_mrc_amt DESC) AS ARREGLO_MRC
FROM dna_usefull_fields AS a
INNER JOIN bills_of_interest_upsell_base AS b ON a.act_acct_cd = b.act_acct_cd AND a.dt BETWEEN b.first_bill_after_upsell AND first_bill_after_upsell + INTERVAL '2' MONTH
GROUP BY a.act_acct_cd
)
,upsell_payments AS (
SELECT  act_acct_cd
        ,TRY(ARRAY_AGG(DATE(DT) ORDER BY DATE(DT))[1]) AS FIRST_PAY_DATE
        ,TRY(ARRAY_AGG(DATE(DT) ORDER BY DATE(DT) DESC)[1]) AS LAST_PAY_DATE
        ,ARRAY_AGG(DATE(DT) ORDER BY DATE(DT)) AS ARREGLO_PAGOS_DATES
        ,ARRAY_AGG(CAST(payment_amt_usd AS DOUBLE) ORDER BY DATE(DT)) AS ARREGLO_PAGOS
        ,ROUND(SUM(CAST(payment_amt_usd AS DOUBLE)),2) AS total_payments_in_3_months
        ,ROUND(SUM(IF(DATE_DIFF('month', DATE(b.first_bill_after_upsell), DATE(dt)) < 1 OR (EXTRACT(DAY FROM b.first_bill_after_upsell) = EXTRACT(DAY FROM DATE(dt)) AND EXTRACT(MONTH FROM b.first_bill_after_upsell) + 1 = EXTRACT(MONTH FROM DATE(dt))) ,CAST(payment_amt_usd AS DOUBLE), NULL)),2) AS total_payments_30_days
        ,ROUND(SUM(IF(DATE_DIFF('month', DATE(b.first_bill_after_upsell), DATE(dt)) < 2 OR (EXTRACT(DAY FROM b.first_bill_after_upsell) = EXTRACT(DAY FROM DATE(dt)) AND EXTRACT(MONTH FROM b.first_bill_after_upsell) + 2 = EXTRACT(MONTH FROM DATE(dt))),CAST(payment_amt_usd AS DOUBLE), NULL)),2) AS total_payments_60_days
        ,ROUND(SUM(IF(DATE_DIFF('month', DATE(b.first_bill_after_upsell), DATE(dt)) < 3 OR (EXTRACT(DAY FROM b.first_bill_after_upsell) = EXTRACT(DAY FROM DATE(dt)) AND EXTRACT(MONTH FROM b.first_bill_after_upsell) + 3 = EXTRACT(MONTH FROM DATE(dt))),CAST(payment_amt_usd AS DOUBLE), NULL)),2) AS total_payments_90_days
FROM "db-stage-prod"."payments_cwp"  AS P
INNER JOIN MRC_upsell AS b ON b.act_acct_cd = CAST(P.account_id AS VARCHAR) AND DATE(P.dt) BETWEEN first_bill_after_upsell AND first_bill_after_upsell + INTERVAL '3' MONTH
GROUP BY act_acct_cd, max_tot_mrc
)

/*

*/
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
SELECT 'Upsell' AS base_classification, 
        upsell_type,
        act_acct_cd,
        tech_flag,
        geography,
        sale_rep,
        first_pd_mix_qty,
        first_rgu_qty,
        last_pd_mix_qty,
        last_rgu_qty,
        max_rgu as last_rgu, 
        upsell_rgus,
        upsell_rgu_with_planchanges,
        upsell_rgus_names,
        rgu_vendidos,
        upsell_update_date, 
        first_bill_after_upsell,
        NULL AS first_installation_date, 
        NULL AS first_bill_created, 
        max_tot_mrc, 
        ARREGLO_MRC, 
        FIRST_PAY_DATE, 
        LAST_PAY_DATE, 
        ARREGLO_PAGOS_DATES, 
        ARREGLO_PAGOS, 
        total_payments_in_3_months, 
        total_payments_30_days, 
        total_payments_60_days, 
        total_payments_90_days, 
        npn_30_flag, 
        npn_60_flag, 
        npn_90_flag, 
        npn_flag
FROM upsell_presummary AS u
UNION ALL
SELECT 'Gross-add' AS base_classification, 
        NULL AS upsell_type,
        act_acct_cd,
        tech_flag,
        geography,
        sale_rep,
        null as first_pd_mix_qty,
        null as first_rgu_qty,
        null as last_pd_mix_qty,
        null as last_rgu_qty,
        max_rgu as last_rgu,
        NULL AS upsell_rgus,
        null as upsell_rgu_with_planchanges,
        null as upsell_rgus_names,
        rgu_vendidos,
        NULL AS upsell_update_date,
        NULL AS first_bill_after_upsell,
        first_installation_date,
        first_bill_created,
        max_tot_mrc,
        ARREGLO_MRC,
        FIRST_PAY_DATE,
        LAST_PAY_DATE,
        ARREGLO_PAGOS_DATES,
        ARREGLO_PAGOS,
        total_payments_in_3_months,
        total_payments_30_days,
        total_payments_60_days,
        total_payments_90_days,
        npn_30_flag,
        npn_60_flag,
        npn_90_flag,
        npn_flag
FROM gross_add_presummary AS g
)

,summary_by_user AS (
SELECT *
FROM MERGING_BASES
INNER JOIN sales_channel_calculation USING(act_acct_cd)
)

,summary_flag as (
SELECT (select input_month from parameters) as sales_month,*, case 
when base_classification = 'Gross-add' then 'Gross Add'
when upsell_type = 'Upsell' and upsell_rgus_names >0 then 'Upsell'
else'Other'  end as movement_flag
, case
when base_classification = 'Gross-add' and last_rgu = 1 then '1P'
when base_classification = 'Gross-add' and last_rgu = 2 then '2P'
when base_classification = 'Gross-add' and last_rgu = 3 then '3P'
when base_classification in ('Upsell', 'Other') and first_rgu_qty = 1 then '1P'
when base_classification in ('Upsell', 'Other') and first_rgu_qty = 2 then '2P'
when base_classification in ('Upsell', 'Other') and first_rgu_qty = 3 then '3P'
else null  end as bunddle_type
, case when base_classification = 'Gross-add' then 0 else first_rgu_qty end as first_rgu_cnt
, case when base_classification = 'Gross-add' then last_rgu else last_rgu_qty end as last_rgu_cnt
from summary_by_user 
)



-- ######################################## Mortlity Rate ###############################################

,forward_months as (
Select date_trunc('MONTH', date(dt)) as month_survival, act_acct_cd, fi_outst_age 
from "db-analytics-prod"."fixed_cwp" 
where date(dt) = date_trunc('MONTH',date(dt)) + interval '1' month - interval '1' day and date(dt) between (select input_month from parameters) and (select input_month from parameters) + interval '12' month
and act_acct_cd in (select distinct act_acct_cd from summary_flag)
)


,acct_panel_surv as (
select act_acct_cd as act_acct_cd_surv,
max(case when month_survival = (select input_month from parameters) + interval '0' month and (fi_outst_age < 90 or fi_outst_age is null) then 1 end) as surv_M0,
max(case when month_survival = (select input_month from parameters) + interval '0' month then fi_outst_age end) as fi_outst_age_M0,
max(case when month_survival = (select input_month from parameters) + interval '1' month and (fi_outst_age <90 or fi_outst_age is null) then 1 end) as surv_M1,
max(case when month_survival = (select input_month from parameters) + interval '1' month then fi_outst_age end) as fi_outst_age_M1,
max(case when month_survival = (select input_month from parameters) + interval '2' month and (fi_outst_age<90 or fi_outst_age is null) then 1 end) as surv_M2,
max(case when month_survival = (select input_month from parameters) + interval '2' month then fi_outst_age end) as fi_outst_age_M2,
max(case when month_survival = (select input_month from parameters) + interval '3' month and (fi_outst_age<90 or fi_outst_age is null) then 1 end) as surv_M3,
max(case when month_survival = (select input_month from parameters) + interval '3' month then fi_outst_age end) as fi_outst_age_M3,
max(case when month_survival = (select input_month from parameters) + interval '4' month and (fi_outst_age<90 or fi_outst_age is null) then 1 end) as surv_M4,
max(case when month_survival = (select input_month from parameters) + interval '4' month then fi_outst_age end) as fi_outst_age_M4,
max(case when month_survival = (select input_month from parameters) + interval '5' month and (fi_outst_age<90 or fi_outst_age is null) then 1 end) as surv_M5,
max(case when month_survival = (select input_month from parameters) + interval '5' month then fi_outst_age end) as fi_outst_age_M5,
max(case when month_survival = (select input_month from parameters) + interval '6' month and (fi_outst_age<90 or fi_outst_age is null) then 1 end) as surv_M6,
max(case when month_survival = (select input_month from parameters) + interval '6' month then fi_outst_age end) as fi_outst_age_M6,
max(case when month_survival = (select input_month from parameters) + interval '7' month and (fi_outst_age<90 or fi_outst_age is null) then 1 end) as surv_M7,
max(case when month_survival = (select input_month from parameters) + interval '7' month then fi_outst_age end) as fi_outst_age_M7,
max(case when month_survival = (select input_month from parameters) + interval '8'  month and (fi_outst_age<90 or fi_outst_age is null) then 1 end) as surv_M8,
max(case when month_survival = (select input_month from parameters) + interval '8' month then fi_outst_age end) as fi_outst_age_M8,
max(case when month_survival = (select input_month from parameters) + interval '9' month and (fi_outst_age<90 or fi_outst_age is null) then 1 end) as surv_M9,
max(case when month_survival = (select input_month from parameters) + interval '9' month then fi_outst_age end) as fi_outst_age_M9,
max(case when month_survival = (select input_month from parameters) + interval '10' month and (fi_outst_age<90 or fi_outst_age is null) then 1 end) as surv_M10,
max(case when month_survival = (select input_month from parameters) + interval '10' month then fi_outst_age end) as fi_outst_age_M10,
max(case when month_survival = (select input_month from parameters) + interval '11' month and (fi_outst_age<90 or fi_outst_age is null) then 1 end) as surv_M11,
max(case when month_survival = (select input_month from parameters) + interval '11' month then fi_outst_age end) as fi_outst_age_M11,
max(case when month_survival = (select input_month from parameters) + interval '12' month and (fi_outst_age<90 or fi_outst_age is null) then 1 end) as surv_M12,
max(case when month_survival = (select input_month from parameters) + interval '12' month then fi_outst_age end) as fi_outst_age_M12
from forward_months 
group by act_acct_cd
)

,join_new_acct_surv as (
select a.*, b.*
from summary_flag a
left join acct_panel_surv b
on a.act_acct_cd = b.act_acct_cd_surv
)

/*
,churners as (
select *,
case when surv_m0 is null then act_Acct_cd else null end as churn_m0,
case when surv_m0 =1 and surv_m1 is null then act_Acct_cd else null end as churn_m1,
case when surv_m1 =1 and surv_m2 is null  then act_Acct_cd else null end  as churn_m2,
case when surv_m2 =1 and surv_m3 is null  then act_Acct_cd else null end  as churn_m3,
case when surv_m3 =1 and surv_m4 is null then act_Acct_cd else null end  as churn_m4,
case when surv_m4 =1 and surv_m5 is null then act_Acct_cd else null end  as churn_m5,
case when surv_m5 =1 and surv_m6 is null then act_Acct_cd else null end  as churn_m6,
case when surv_m6 =1 and surv_m7 is null then act_Acct_cd else null end  as churn_m7,
case when surv_m7 =1 and surv_m8 is null then act_Acct_cd else null end  as churn_m8,
case when surv_m8 =1 and surv_m9 is null then act_Acct_cd else null end  as churn_m9,
case when surv_m9 =1 and surv_m10 is null then act_Acct_cd else null end  as churn_m10,
case when surv_m10 =1 and surv_m11  is null then act_Acct_cd else null end  as churn_m11,
case when surv_m11 =1 and surv_m12 is null then act_Acct_cd else null end  as churn_m12
from join_new_acct_surv
)
*/


select count(distinct act_acct_cd), sum(surv_m0), sum(surv_m1), sum(surv_m2), sum(surv_m3), sum(surv_m4), sum(surv_m5), sum(surv_m6), sum(surv_m7), sum(surv_m8), sum(surv_m9), sum(surv_m10) from join_new_acct_surv where movement_flag = 'Gross Add'

/*
select count(distinct act_acct_cd)
        ,count(distinct churn_M0)
        ,count(distinct churn_M1)
        ,count(distinct churn_M2)
        ,count(distinct churn_M3)
        ,count(distinct churn_M4)
        ,count(distinct churn_M5)
        ,count(distinct churn_M6)
        ,count(distinct churn_M7)
        ,count(distinct churn_M8)
        ,count(distinct churn_M9)
        ,count(distinct churn_M10)
        ,count(distinct churn_M11)
        ,count(distinct churn_M12)
        from churners where movement_flag = 'Gross Add'
--select count(distinct act_acct_cd), sum(surv_m0),sum(surv_m1),sum(surv_m2),sum(surv_m3),sum(surv_m4),sum(surv_m5),sum(surv_m6),sum(surv_m7),sum(surv_m8),sum(surv_m9),sum(surv_m10) from join_new_acct_surv where movement_flag= 'Gross Add'

*/














--select distinct (select input_month from parameters)as month,act_acct_cd, tech_flag, geography, first_rgu_cnt, last_rgu_cnt, upsell_rgus_names,bunddle_type, movement_flag,fi_sales_channel,fi_sales_channel_sub,sale_rep, npn_30_flag, npn_60_flag,npn_90_flag, npn_flag from summary_flag --where movement_flag = 'Gross Add' --upsell_rgus < 0
/*
select (select input_month from parameters) as month, 
        movement_flag,
        count(distinct act_acct_cd) as sales,
        sum(rgu_vendidos) as rgus_virgilio,
        sum(case when movement_flag = 'Gross Add' then last_rgu else upsell_rgus end) as rgus_pdmix,
        sum(case when movement_flag = 'Gross Add' then last_rgu else upsell_rgu_with_planchanges end) as rgus_planChanges,
        sum(case when movement_flag = 'Gross Add' then last_rgu else upsell_rgus_names end) as rgus_names
        from summary_flag 
        group by 1,2
*/
--select movement_flag, count(distinct act_acct_cd), sum(last_rgu_cnt - case when first_rgu_cnt is null then 0 else first_rgu_cnt end ) from summary_flag group by 1

/*
,gross_info as (
select act_acct_cd from summary_flag where movement_flag = 'Gross Add'
)

,FMC_info as (
select month,fixedaccount,fixedmainmovement,fixedspinmovement,waterfall_flag,b_numrgus,e_numrgus,rejoinerflag 
from "lla_cco_int_ana_dev"."cwp_fmc_churn_dev" where month= date(dt) and month = (select input_month from parameters) and fixedmainmovement in ('4.New Customer', '5.Come Back to Life','8.Rejoiner-GrossAdd Gap') and waterfall_flag ='Gross Adds'
)



,join_gross_fmc as (
select  act_acct_cd,fixedaccount, fixedmainmovement,fixedspinmovement, 
case when act_acct_cd is not null and fixedaccount is not null then fixedaccount else null end as both,
case when act_acct_cd is null and fixedaccount is not null then fixedaccount else null end as fmc_only,
case when act_acct_cd is not null and fixedaccount is null then act_acct_cd else null end as gross_only
from gross_info full outer join FMC_info on act_acct_cd = fixedaccount 
)

--select fixedmainmovement,fixedspinmovement,count(distinct both) as both, count(distinct fmc_only) as fmc_only, count(distinct gross_only) as gross_only from join_gross_fmc group by 1,2

--Select distinct dt,dna.act_acct_cd,pd_tv_prod_cd,pd_vo_prod_cd,pd_vo_prod_nm,pd_bb_prod_cd,fi_outst_age,act_cust_strt_dt,pd_mix_cd,fixedmainmovement from "db-analytics-prod"."fixed_cwp" dna inner join join_gross_fmc j on dna.act_acct_cd = j.fixedaccount where fmc_only is not null and date(dt) = date('2022-08-31') order by 2,1

,dna_info as (
select dt, act_acct_cd, first_value(dt)over(partition by act_acct_cd order by dt) ,pd_tv_prod_cd,pd_vo_prod_cd,pd_vo_prod_nm,pd_bb_prod_cd,fi_outst_age,act_cust_strt_dt,pd_mix_cd from "db-analytics-prod"."fixed_cwp"
)
 --select act_acct_cd from join_gross_fmc where gross_only is not null
 
, dna_gross as (
Select distinct dt,dna.act_acct_cd, first_value(dt)over(partition by dna.act_acct_cd order by dt) ,pd_tv_prod_cd,pd_vo_prod_cd,pd_vo_prod_nm,pd_bb_prod_cd,fi_outst_age,act_cust_strt_dt,pd_mix_cd,fixedmainmovement
from dna_info dna
inner join join_gross_fmc j on dna.act_acct_cd = j.act_acct_cd where gross_only is not null
)

select count(distinct act_acct_cd) from dna_gross where dt = '2023-01-31' and (fi_outst_age is null or fi_outst_age <90) 
--select fixedmainmovement, count(distinct fixedaccount) from FMC_info where fixedaccount in (select * from users_gross_no_fmc) group by 1 
*/
/*
,usuarios_atipicos as (
select distinct act_acct_cd,first_dna,last_dna from (select dt,act_acct_cd,first_value(dt)over(partition by act_acct_cd order by dt) as first_dna,first_value(dt)over(partition by act_acct_cd order by dt desc) as last_dna,pd_mix_cd from "db-analytics-prod"."fixed_cwp") inner join users_fmc_no_gross on act_acct_cd = fixedaccount
)
select first_dna,count(distinct act_Acct_cd) from usuarios_atipicos group by 1 
*/
