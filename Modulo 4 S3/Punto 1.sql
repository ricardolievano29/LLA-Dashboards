With 
-- ususarios activos, residenciales en panama en 2022
usuarios_activos as (
select date_trunc('MONTH', date(dt)) as mes,act_acct_cd  
from "db-analytics-prod"."fixed_cwp"
where date(dt) = date_trunc('MONTH',date(dt)) -- usuarios en la base al primer dia del mes
and date(dt) >= date('2022-01-01') -- registros solo del 2022
and act_cust_typ_nm = 'Residencial'-- clientes residenciales unicamente 
and date_diff('DAY',  cast (concat(substr(cast(oldest_unpaid_bill_dt as VARCHAR), 1,4),'-',substr(cast(oldest_unpaid_bill_dt as VARCHAR), 5,2),'-', substr(cast(oldest_unpaid_bill_dt as VARCHAR), 7,2)) as date)  , cast(dt as date)) is null 
or date_diff('DAY',  cast (concat(substr(cast(oldest_unpaid_bill_dt as VARCHAR), 1,4),'-',substr(cast(oldest_unpaid_bill_dt as VARCHAR), 5,2),'-', substr(cast(oldest_unpaid_bill_dt as VARCHAR), 7,2)) as date)  , cast(dt as date)) <90 
)
-- llamadas por reclamos e identificacion de one callers y repeated callers
,reclamos_mes as (
Select distinct date_trunc('MONTH', date(interaction_start_time)) as mes, account_id, case when count(distinct interaction_id) = 1 then 'One caller' when count(distinct interaction_id) > 1 then 'Repeated caller' else null end as caller_flag
from "db-stage-prod"."interactions_cwp"
where interaction_purpose_descrip  = 'CLAIM'
and date_trunc('MONTH',date(interaction_start_time))>=date('2022-01-01')
group by 1,2 order by 1
)

------------------------------------------ Resultados ----------------------------------------------------------

select distinct a.mes, c.caller_flag,count(distinct a.act_acct_cd) as clientes
from usuarios_activos a inner join reclamos_mes c on a.act_acct_cd = c.account_id and a.mes = c.mes
group by 1,2 order by 1,2
