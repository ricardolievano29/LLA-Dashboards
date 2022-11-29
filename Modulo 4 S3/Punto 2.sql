with
actives_dy as(
select date(dt) as dy,act_acct_cd
from "db-analytics-prod"."fixed_cwp" 
where date(dt) between date('2022-06-30') and date('2022-09-01') and act_cust_typ_nm = 'Residencial' and fi_outst_age<90
order by 1
)
,last_active as(
select distinct act_acct_cd,first_value(dy)over(partition by act_acct_cd order by dy desc) as last_dy from actives_dy
)

select distinct last_dy, count(distinct act_acct_cd) as churners from last_active where last_dy between date('2022-07-01') and date('2022-08-31') group by 1 order by 1

 
