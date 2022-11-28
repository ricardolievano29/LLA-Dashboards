with
----------------------------------------------------Usuarios activos,residencial a inicio de mes---------------------------------------------
usuarios_activos as(
Select distinct date_trunc('MONTH',date(dt)) as mnth,  act_acct_cd
from "db-analytics-prod"."fixed_cwp"
where date(dt)>=date('2022-01-01') -- and date(dt)=date_trunc('MONTH',date(dt))
and act_cust_typ_nm = 'Residencial'
and fi_outst_age <90
)
----------------------------------------------------- LLamadas por cliente or mes ----------------------------------------------------------
,Calls_per_mnth as(
select distinct date_trunc('MONTH',date(interaction_start_time)) as mnth, account_id, count(distinct interaction_id) as calls
from "db-stage-prod"."interactions_cwp"
where interaction_purpose_descrip = 'CLAIM'
group by 1,2
)

----------------------------------------------------- Clasificacion de callers -------------------------------------------------------------
,Caller_flag as (
select distinct mnth,account_id, case when calls = 1 then 'Single caller' when calls > 1 then 'Repeated caller' else null end as Repeated_flag
from Calls_per_mnth
)

------------------------------------------------------Agrupación por tipo de clientes-----------------------------------------------------------
select distinct a.mnth, c.Repeated_flag, count(distinct a.act_acct_cd) as clients
from usuarios_activos a inner join Caller_flag c on a.act_acct_cd = c.account_id and a.mnth  = c.mnth
group by 1,2 order by 1,2


