-- ==============================================================================================
-- comparación entre las el archuvi de Digital Sales y el Gross Adds table,
-- identificación de las ventas que estan en digital y no en Gross Adds,
-- Revision del comportamiento de estas cuentas en el DNA
-- Conclusión: Las cuentas que no aparecen en Gross Adds y si en el archivo de Digital Sales 
-- Son efectivamente cambios de planes que no se registraron en Gross Adds
-- ==============================================================================================

WITH

C_file as (
select * from "lla_cco_int_san"."cwp_ext_digital_sales"
)

,Gross as (
select sales_month,cuenta,rgus_vendidos,sales_channel from "db-stage-prod"."cwp_gross_adds_fijo" where sales_month = '2022-12-01'
)

,users_in_gross as (
select distinct  sales_month, cuenta, cod_producto,sales_channel from c_file inner join gross on cuenta = cod_cuenta and sales_channel <> 'C. Digital' 
)

,users_not_in_gross_dec as (
select sales_channel, cod_cuenta,cod_producto from C_file full outer join Gross on cuenta = cod_cuenta  where cuenta is null
)


,dna_join as (
select distinct a.dt,a.act_acct_cd,a.pd_tv_prod_cd,a.pd_vo_prod_cd,a.pd_bb_prod_cd,
b.pd_tv_prod_cd as prev_tv,b.pd_vo_prod_cd as prev_vo,b.pd_bb_prod_cd as prev_bb,
case when a.pd_tv_prod_cd <> b.pd_tv_prod_cd or a.pd_vo_prod_cd <> b.pd_vo_prod_cd or a.pd_bb_prod_cd <> b.pd_bb_prod_cd then 1 else 0 end  as upgrade,
first_dna from 
(select date(dt) as dt,act_acct_cd,pd_tv_prod_cd,pd_vo_prod_cd,pd_bb_prod_cd, first_value(dt)over(partition by act_acct_cd order by dt) as first_dna from "db-analytics-prod"."fixed_cwp") a 
inner join
(select date(dt) as dt,act_acct_cd,pd_tv_prod_cd,pd_vo_prod_cd,pd_bb_prod_cd from "db-analytics-prod"."fixed_cwp") b 
on a.act_acct_cd = b.act_acct_cd and a.dt = b.dt + interval '1' day where a.dt between date('2022-06-01') and date('2023-01-31') and a.act_acct_cd in (select cast(cod_cuenta as varchar) from users_not_in_gross_dec)
order by 2,1
)

select distinct act_acct_cd
from dna_join where upgrade = 1 and date(dt) between date('2022-12-01') and date('2023-01-01')


 select count(distinct act_acct_cd) from dna_join where act_acct_cd in (Select cast(cod_cuenta as varchar) from users_not_in_gross_dec)
 ,results as(
select distinct --first_value(dt) over (partition by a.act_acct_cd,a.pd_tv_prod_cd,a.pd_vo_prod_cd,a.pd_bb_prod_cd,prev_tv, prev_vo,  prev_bb,cod_producto order by dt) as 
dt ,
act_acct_cd,a.pd_tv_prod_cd,a.pd_vo_prod_cd,a.pd_bb_prod_cd,cod_producto,
prev_tv, prev_vo,  prev_bb, 
case when (cod_producto =pd_tv_prod_cd and cod_producto <> prev_tv) or   
(cod_producto =pd_vo_prod_cd and cod_producto <> prev_vo) or (cod_producto =pd_bb_prod_cd and cod_producto <> prev_bb) then 1 else 0 end as changes
--, row_number()over(partition by dt,act_acct_cd,pd_tv_prod_cd,pd_vo_prod_cd,pd_bb_prod_cd order by dt)
from dna_join a left  join C_file on a.act_acct_cd = cast(cod_cuenta as varchar) 
--  and (cod_producto = (a.pd_tv_prod_cd) or
--  cod_producto = (a.pd_vo_prod_cd) or
--  cod_producto = (a.pd_bb_prod_cd) )
order by 2,1
limit 100
)

select distinct count(distinct act_acct_cd) from results where changes=1

--select count(distinct act_acct_cd) from results 

-- select * from dna_join where upgrade = 1 and date_trunc('MONTH',date(dt)) = date('2022-12-01')
-- select distinct count(distinct act_acct_cd) from dna_join inner join users_not_in_gross_dec on cast(cod_cuenta as varchar) = act_acct_cd and cod_producto in (pd_tv_prod_cd,pd_vo_prod_cd,pd_bb_prod_cd) where upgrade  = 1
-- select distinct date_trunc('MONTH',date(dt)),count(distinct act_acct_cd) from dna_join where upgrade  = 1 group by 1 

--  select sales_month, count( distinct cuenta) from "db-stage-prod"."cwp_gross_adds_fijo" inner join users_not_in_gross_dec on cuenta = cod_cuenta group by 1 

-- select distinct count(distinct case when cod_producto = pd_tv_prod_cd or cod_producto = pd_vo_prod_cd or cod_producto = pd_bb_prod_cd then cod_cuenta else null end )from dna_join
