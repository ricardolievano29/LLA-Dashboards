WITH
-- se ponen las flags para identificar el tenure de los clientes 
Base_AUX as (
SELECT
dt,
extract(month from cast(dt as date)) as mes,
act_acct_cd,
case
when date_diff('day', cast(act_acct_inst_dt as date), cast(dt as date))<180 then 'early tenure' 
when date_diff('day', cast(act_acct_inst_dt as date), cast(dt as date))>=180 and date_diff('day', cast(act_acct_inst_dt as date), cast(dt as date))<360 then 'mid tenure'
when date_diff('day', cast(act_acct_inst_dt as date), cast(dt as date))>=360 then 'late tenure'
else 'aja' end as tenure
FROM "db-analytics-prod"."fixed_cwp" 
 where act_cust_typ_nm = 'Residencial'
 and extract(year from cast(dt as date)) = 2022
)
-- se toma el primer dia del mes del cual se tienen datos, no se tinen datos para el primero de marzo

SELECT
mes,
extract(day from cast(dt as date)) as primer_dia,
tenure,
count(distinct act_acct_cd) as clientes
from Base_AUX 
where case when extract(month from cast(dt as date)) != 3 then extract(day from cast(dt as date)) = 1 else extract(day from cast(dt as date)) = 2 end

group by 1,2,3
order by 1,2
