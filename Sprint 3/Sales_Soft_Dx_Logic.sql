WITH 
Parameters AS (
SELECT
    DATE('2022-09-01') AS start_date,
    DATE('2022-11-01') AS end_date
)
,mora_error as(
    select distinct Month,dt,act_acct_cd,mora,prev_mora,next_mora,Bill_DT_M0,
        Oldest_Unpaid_Bill_DT,act_cust_strt_dt
        ,case 
            when ((mora-prev_mora)>2 and (mora-next_mora)>2 ) or ( (mora-prev_mora)<-2 and (mora-next_mora)<-2 ) then 1 
            else 0 end as mora_salto
    from(
        select distinct date_trunc('Month',date(dt)) as Month,Fi_Outst_Age as mora,dt, act_acct_cd,Bill_DT_M0,
            Oldest_Unpaid_Bill_DT,act_cust_strt_dt
            ,lag(fi_outst_age) over(partition by act_acct_cd order by dt desc) as next_mora
            ,lag(fi_outst_age) over(partition by act_acct_cd order by dt) as prev_mora
        FROM "db-analytics-dev"."dna_fixed_cr"
        WheRE (act_cust_typ='RESIDENCIAL' or act_cust_typ='PROGRAMA HOGARES CONECTADOS') and (act_acct_stat='ACTIVO' or act_acct_stat='SUSPENDIDO')
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

,SO_INFO as(
    SELECT DISTINCT SC.account_name,
                    FIRST_VALUE(order_id) OVER(PARTITION BY SC.account_name ORDER BY completed_date ASC) AS FI_order,
                    FIRST_VALUE(DATE(order_start_date)) OVER(PARTITION BY SC.account_name ORDER BY completed_date asc) AS FI_order_start_date,
                    FIRST_VALUE(DATE(completed_date)) OVER(PARTITION BY SC.account_name ORDER BY completed_date ASC) as FI_order_completed_date
    FROM "db-stage-dev"."so_cr" as sc
    WHERE
    order_type = 'INSTALACION' 
    AND order_status = 'FINALIZADA'
),
Sales_data as(
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

,sales as (
select distinct date_trunc('MONTH', FI_order_completed_date) as month, count(distinct account_name)  as ventas
--from SO_INFO 
FROM INFO_QUALITY
inner join mora_arreglada on account_name = act_acct_cd 
-- ESTE WHERE ES NECESARIO ? SI YA ESTÁS FILTRANDO EN INFO_QUALITY CREERÍA QUE NO NECESITAS MÁS FILTROS DE FECHAS. LO DEJO A TU CONSIDERACIÓN IGUAL TAMBIÉN TE RECOMENDARÍA USAR EL PARÁMETRO PARA EL INICIO DE LAS FECHAS
-- where  date_trunc('MONTH', FI_order_completed_date)  >= date('2022-08-01')
-- CB RECOMENDACIÓN -- where  date_trunc('MONTH', FI_order_completed_date)  >= (SELECT start_date FROM Paramters)
group by 1
)


,cuentas_fecha as (
SELECT  reference_month
        ,first_dna_date
        ,COUNT(DISTINCT account_name) AS cant_users

FROM INFO_QUALITY
group by 1,2
-- order by 3 asc
)

,fechas_maxima as(
select reference_month, first_dna_date, cant_users, row_number() over (partition by reference_month order by cant_users desc) as relevante 
from cuentas_fecha
)
,candidatos_soft_dx as (
select b.first_dna_date,a.* 
from INFO_QUALITY a 
inner join fechas_maxima b on a.reference_month = b.reference_month and relevante =1
)
,soft_dx as (
    select reference_month, account_name
    ,case when max(mora_fix)>= 26 and max(mora_fix)<90 then account_name else null end as Flag_soft_dx
     from mora_arreglada a inner join candidatos_soft_dx b on act_acct_cd = account_name 
     group by 1,2

)
select month, ventas, max(cant_users) as base, count(distinct Flag_soft_dx) as Soft_dx 
-- from base_soft_dx a 
from sales a --on a.month=b.month 
left join cuentas_fecha as cf on cf.reference_month = a.month
left join soft_dx c on a.month = c.reference_month 
group by 1,2--,3
