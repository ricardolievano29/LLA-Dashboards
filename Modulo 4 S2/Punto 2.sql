with usuarios as (Select distinct dt,account_id,date_diff('DAY',  cast (concat(substr(oldest_unpaid_bill_dt, 1,4),'-',substr(oldest_unpaid_bill_dt, 5,2),'-', substr(oldest_unpaid_bill_dt, 7,2)) as date), cast(dt as date)) as fi_outst_age
from "db-analytics-prod"."tbl_postpaid_cwc"
where org_id = '338'and account_type = 'Residential' and date_trunc('MONTH', DATE (dt)) = date('2022-10-01')),
active_tag as(select dt,account_id, case when fi_outst_age >=90 then 'Inactive' when fi_outst_age <90 then 'Active' else null end as active_inactive from usuarios)

select distinct date_trunc('MONTH', date(v.dt)) as mes, count(distinct v.account_id) as num_clientes_service_orders
from active_tag v left join "db-stage-dev"."so_hdr_cwc"  s 
on  cast(v.account_id as VARCHAR) = CAST(s.account_id AS VARCHAR)
where v.active_inactive = 'Active' and order_type = 'DEACTIVATION'
and date_trunc('MONTH',date(v.dt)) = date_trunc('MONTH',s.order_start_date)

group by 1 order by 1
