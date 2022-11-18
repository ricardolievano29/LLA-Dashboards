select
extract(month from order_start_date) as mes,
count( distinct order_id) as orders
FROM "db-stage-dev"."so_hdr_cwc"
where extract(year from order_start_date) = 2022
and account_type = 'Residential'
and org_cntry = 'Jamaica'
group by 1
order by 1
