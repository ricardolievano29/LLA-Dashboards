with usuarios as(select distinct dt, date_trunc('MONTH', DATE(dt)) as mes ,act_acct_cd as users
FROM "db-analytics-prod"."fixed_cwp"
where date(dt)>=date('2022-01-01') and date(dt)=date_trunc('MONTH',date(dt)) and pd_bb_prod_cd is not null
)

select distinct u.mes,count(distinct u.users) as truckrolls
from usuarios u left join "db-stage-prod"."interactions_cwp" i on users = i.account_id 
and u.mes = date_trunc('MONTH',i.interaction_start_time)
--where date(u.dt)= date_trunc('MONTH', date(u.dt))
and date(u.dt)>= date('2022-01-01')
and i.interaction_purpose_descrip = 'TRUCKROLL'
group by 1 order by 1
