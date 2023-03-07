WITH
parameters as (
select date('2022-09-01') as input_month
)

,a1 as(
SELECT *
FROM ( 
    SELECT act_acct_cd, min(date(dt)) AS first_dna_date, min(fi_outst_age) as min_fi_outst, max(fi_bill_dt_m0) as fi_bill_dt_m0
    FROM "db-analytics-prod"."fixed_cwp"
    WHERE act_cust_typ_nm = 'Residencial'
        --AND DATE_TRUNC('MONTH', act_acct_inst_dt) = (SELECT input_month FROM Parameters) /*Debemos ajustar este criterio para que sea sobre el DT*/
        AND DATE(dt) BETWEEN (SELECT input_month FROM Parameters) - interval '6' MONTH AND  (SELECT input_month FROM Parameters) + INTERVAL '1' MONTH
    GROUP BY act_acct_cd
    ) 
WHERE date_trunc('month', first_dna_date) = (SELECT input_month FROM parameters)
)


,FMC_info as (
select month,fixedaccount,fixedmainmovement,fixedspinmovement,waterfall_flag,b_numrgus,e_numrgus,rejoinerflag 
from "lla_cco_int_ana_dev"."cwp_fmc_churn_dev" 
where month= date(dt) and month = (select input_month from parameters)
)

,faltantas_sales as (
select act_acct_cd, min_fi_outst,fi_bill_dt_m0 from a1 left join FMC_info on fixedaccount = act_acct_cd where fixedaccount is null
)

,faltantes as (
select dt,a.act_acct_cd,b.min_fi_outst,a.fi_bill_dt_m0, fi_tot_mrc_amt from "db-analytics-prod"."fixed_cwp" a inner join faltantas_sales b on a.act_acct_cd = b.act_acct_cd
)

,bills_of_interest AS (
SELECT act_acct_cd,
    DATE(TRY(FILTER(ARRAY_AGG(fi_bill_dt_m0 ORDER BY DATE(dt)), x -> x IS NOT NULL)[1])) AS first_bill_created
FROM faltantes
GROUP BY act_acct_cd 
)
,mrc_calculation AS (
SELECT sb.act_acct_cd,
        MIN(bi.first_bill_created) AS first_bill_created, 
        MAX(fi_tot_mrc_amt) AS max_tot_mrc, 
        ARRAY_AGG(DISTINCT fi_tot_mrc_amt ORDER BY fi_tot_mrc_amt DESC) AS ARREGLO_MRC
FROM faltantes AS sb
INNER JOIN bills_of_interest AS bi ON sb.act_acct_cd = bi.act_acct_cd AND date(sb.dt) BETWEEN first_bill_created AND first_bill_created + INTERVAL '2' MONTH
GROUP BY sb.act_acct_cd
)

/*
Input: sales_base y mcr_calculation
Logica: agregar las columnas de firstbill created y regus vendidos (gross table) y rgus presentes en el dna
*/
,first_cycle_info AS (
SELECT  sb.act_acct_cd
        ,MIN(first_bill_created) AS first_bill_created
        ,TRY(ARRAY_AGG(ARREGLO_MRC)[1]) AS ARREGLO_MRC
        ,MAX(max_tot_mrc) AS max_tot_mrc
FROM faltantes AS sb
INNER JOIN mrc_calculation AS mrcc ON mrcc.act_acct_cd = sb.act_acct_cd
WHERE DATE(sb.fi_bill_dt_m0) = mrcc.first_bill_created
GROUP BY sb.act_acct_cd
)
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

select count(distinct npn_flag) from gross_add_presummary
