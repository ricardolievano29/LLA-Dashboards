With 
-- ususarios activos, residenciales en panama en 2022
usuarios_activos as (
select distinct date_trunc('MONTH', date(dt)) as mes,act_acct_cd  
from "db-analytics-prod"."fixed_cwp"
where --date(dt) = date_trunc('MONTH',date(dt)) and 
date(dt) >= date('2022-01-01') -- registros solo del 2022
and act_cust_typ_nm = 'Residencial'-- clientes residenciales unicamente 
and (fi_outst_age is null or fi_outst_age <90 ) --- clientes que no tienen mas de 90 dias de mora
)

,dias_deactivation as(
select distinct
a.act_acct_cd,
date(i.interaction_start_time) as fecha_interaction,
date(s.order_start_date) as fecha_dx,
date_diff('DAY',date(s.order_start_date),date(i.interaction_start_time)) as dias_dx
from usuarios_activos a inner join "db-stage-prod"."interactions_cwp" i on a.act_acct_cd = i.account_id and a.mes = date_trunc('MONTH',i.interaction_start_time)
    inner join "db-stage-dev"."so_hdr_cwp" s on s.account_id = cast(a.act_acct_cd as BIGINT) and a.mes = date_trunc('MONTH',order_start_date)
    where date(s.order_start_date) >= date('2022-01-01')
    and date(i.interaction_Start_time)>= date('2022-01-01')
    and s.order_type = 'DEACTIVATION'
)

select distinct count(distinct act_acct_cd) as usuarios from dias_deactivation where dias_dx <=40 


