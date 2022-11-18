
-- se toma el primer dia del mes del cual se tienen datos, no se tinen datos para el primero de marzo
Select
extract(month from cast(dt as date)) as mes,
(extract(day from cast(dt as date))) as primer_dia,
count(distinct act_acct_cd ) as usuarios

FROM "db-analytics-prod"."fixed_cwp"

 where act_cust_typ_nm = 'Residencial'
 and extract(year from cast(dt as date)) = 2022
 and case when extract(month from cast(dt as date)) != 3 then extract(day from cast(dt as date)) = 1 else extract(day from cast(dt as date)) = 2 end
group by 1,2
order by 1
