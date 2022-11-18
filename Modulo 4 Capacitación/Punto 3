select
interaction_purpose_descrip,
count(account_id) as interacciones
from "db-stage-prod"."interactions_cwp" 
where extract(month from interaction_start_time)  = 8
and extract(year from interaction_start_time) = 2022
group by 1
order by 2
