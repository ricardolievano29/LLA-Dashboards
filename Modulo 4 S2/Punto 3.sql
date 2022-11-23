with
usuarios as(
Select distinct dt, act_acct_cd as users
FROM "db-analytics-prod"."fixed_cwp"
where date(dt)>=date('2022-01-01')
and date(dt)=date_trunc('MONTH',date(dt))
)

select distinct date_trunc('MONTH', date(u.dt)) as mes,count(distinct u.users) as service_orders_interactions
from usuarios u left join "db-stage-prod"."interactions_cwp" i on users = i.account_id and date_trunc('MONTH',date(u.dt)) = date_trunc('MONTH',date(i.dt))
    left join "db-stage-dev"."so_hdr_cwp" s on users =  cast(s.account_id as VARCHAR) and date_trunc('MONTH',date(u.dt)) = date_trunc('MONTH',date(s.dt))
and date(u.dt)>= date('2022-01-01')
and i.account_id is not null and s.account_id is not null
group by 1 order by 1
