
------------------------------ SPRINT 5 -----------------------------------------------------

WITH

FmcTable AS (
SELECT DISTINCT * FROM "lla_cco_int_san"."cr_fmc_table"
)

------------------------------------ One time and repeated callers --------------------------
,dup_fix AS(
SELECT *, row_number() OVER (PARTITION BY account_id, cast(interaction_start_time AS DATE),interaction_purpose_descrip ORDER BY interaction_start_time DESC) AS row_num
FROM "db-stage-dev"."interactions_cabletica"
WHERE (account_type='RESIDENCIAL' or account_type='PROGRAMA HOGARES CONECTADOS') 
AND date_trunc('Month',interaction_start_time)>=DATE('2022-01-01')-- and interaction_status <> 'ANULADA'
)
,Interactions_Fields as(
  select distinct *,date_trunc('Month',interaction_start_time)  as month,account_id AS ContratoInteractions
  From (select * from dup_fix where row_num=1)
)

,Last_Interaction as (
  select account_id AS last_account,
  first_value(dt) over(partition by account_id,date_trunc('Month',date(dt)) order by dt desc) as last_interaction_date
  From (select * from dup_fix where row_num=1)
  WHERE (account_type='RESIDENCIAL' or account_type='PROGRAMA HOGARES CONECTADOS') and date_trunc('Month',interaction_start_time)>=date('2022-01-01') and interaction_status <> 'ANULADA'
        AND interaction_purpose_descrip NOT IN ('VENTANILLA','DESINSTALACION','CORPORATIVO','VENCIMIENTO PROMOCION','*NO DEFINIDO*','SUSPENSIONES',
'CREACION DE FACTURA','VENTAS')
)

,Join_last_interaction as(
  select distinct contratointeractions,interaction_id, dt as interaction_date,date_trunc('Month',date(last_interaction_date)) as InteractionMonth,last_interaction_date,
  date_add('day',-60,date(last_interaction_date)) as window_day
  From interactions_fields w inner join last_interaction l
  on w.contratointeractions=l.last_account
)


,Interactions_Count as(
  select distinct InteractionMonth,Contratointeractions,count(distinct interaction_id) as Interactions
  From join_last_interaction
  where date(interaction_date) between window_day and date(last_interaction_date)
  group by 1,2
)

,Interactions_tier as(
  select *,
  case when Interactions=1 Then contratointeractions else null end as OneCall,
  case when Interactions=2 Then contratointeractions else null end as TwoCalls,
  case when Interactions>=3 Then contratointeractions else null end as MultipleCalls
  From Interactions_Count
)
,RepeatedCallsMasterTable AS(
  SELECT f.*,OneCall,TwoCalls, MultipleCalls
  FROM FMCTable f LEFT JOIN Interactions_tier
  ON Contratointeractions=Fixed_Account AND Month=InteractionMonth
)

--------------------------------------- Users with tickets -------------------------------------

,Tiquetes AS(
SELECT account_id AS Contrato, interaction_start_time AS Fecha_Tiquete, Date_Trunc('Month',date(interaction_start_time)) AS Mes_Tiquete, Interaction_id
    FROM (select * from dup_fix where row_num=1)
    WHERE (account_type='RESIDENCIAL' or account_type='PROGRAMA HOGARES CONECTADOS') and interaction_status <> 'ANULADA'
and interaction_purpose_descrip IN (
    'AVERIAS',
    'SIN SERVICIO INTERNET',
    'INTERRUPCION CONSTANT SERVICIO',
    'EQUIPO INTEGRADO',
    'PROB VELOCIDAD',
    'SIN SEÑAL',
    'OTRO INTERNET',
    'CONTROL REMOTO',
    'SIN SERVICIO TV',
    'PROB STB DIG',
    'SIN SEÑAL UNO/VARIOS CH DVB',
    'PROB CABLE MODEM',
    'MENSAJE ERROR DVB',
    'PROB STB DVB',
    'MENSAJE ERROR',
    'INTERNET/DIGITAL',
    'CALIDAD SEÑAL',
    'TV/INTERNET/DIGITAL',
    'SIN SERVICIO TODOS LOS CH DVB',
    'SIN SERVICIO TELEFONIA',
    'CONTROL REMOTO DVB',
    'AVERIA',
    'PROB STB',
    'SEÑAL LLUVIOSA/RAYAS TV',
    'CONEXION',
    'TV/INTERNET',
    'FALTAN CH PARRILLA BASICA DVB',
    'NIVELES SEÑAL INCORRECTOS INT.',
    'TV/DIGITAL',
    'SIN SEÑAL UNO/VARIOS CH',
    'SIN SERVICIO INT',
    'PROB ROUTER CT',
    'CALIDAD SEÑAL DVB'
)
)

,Tiquetes_Mes AS (
SELECT Contrato, Mes_Tiquete, Count(Distinct interaction_id) AS N_Tiquetes_Mes, Max(Fecha_Tiquete) AS Max_Apertura, 
Date_add('Day',-60,MAX(date(FECHA_TIQUETE))) AS MAX_APERTURA_60D
    FROM Tiquetes
    GROUP BY 1,2
    ORDER BY 2, 3 DESC
)

-- lo que hacemos aquí es buscar por cada mes los usuarios que tengan llamadas en los 60 días anteriores
,Tiquetes_60D AS (
    SELECT m.*,l.FECHA_TIQUETE AS FECHA_TIQUETE_ANT, l.interaction_id
    FROM TIQUETES_MES AS m
    LEFT JOIN TIQUETES AS l
        ON ((m.CONTRATO = l.CONTRATO) AND (l.FECHA_TIQUETE BETWEEN m.MAX_APERTURA_60D AND date(m.MAX_APERTURA)))
)

,TIQUETES_GR AS (
    SELECT MES_TIQUETE,CONTRATO,COUNT(DISTINCT interaction_id) AS N_TIQUETES_60D,
        CASE -- marca para el número de llamadas en los últimos 60 días
            WHEN COUNT(DISTINCT interaction_id) = 1 THEN '1'
            WHEN COUNT(DISTINCT interaction_id) = 2 THEN '2'
            WHEN COUNT(DISTINCT interaction_id) >= 3 THEN '3+'
        ELSE NULL END AS FLAG_TIQUETES_60D
    FROM TIQUETES_60D
    GROUP BY 1,2
    ORDER BY 1, 3 DESC
)
,OneTicket AS (
    SELECT MES_TIQUETE AS TICKET_MONTH, FLAG_TIQUETES_60D AS TICKETS_FLAG, Contrato AS ContratoTicket
    FROM TIQUETES_GR
    WHERE FLAG_TIQUETES_60D='1'
    GROUP BY 1,2,3
    ORDER BY 1
)

,TwoTickets AS (
    SELECT MES_TIQUETE AS TICKET_MONTH, FLAG_TIQUETES_60D AS TICKETS_FLAG, Contrato AS ContratoTicket
    FROM TIQUETES_GR
    WHERE FLAG_TIQUETES_60D='2'
    GROUP BY 1,2,3
    ORDER BY 1
)

,MultipleTickets AS (
    SELECT MES_TIQUETE AS TICKET_MONTH, FLAG_TIQUETES_60D AS TICKETS_FLAG,Contrato AS ContratoTicket
    FROM TIQUETES_GR
    WHERE FLAG_TIQUETES_60D='3+'
    GROUP BY 1,2,3
    ORDER BY 1
)

,OneTicketMasterTable AS(
  SELECT f.*,ContratoTicket AS OneTicket
  FROM RepeatedCallsMasterTable f LEFT JOIN OneTicket ON ContratoTicket=Fixed_Account AND Month=TICKET_MONTH
)

,TwoTicketsMasterTable AS(
  SELECT f.*,ContratoTicket AS TwoTickets
  FROM OneTicketMasterTable f LEFT JOIN TwoTickets ON ContratoTicket=Fixed_Account AND Month=TICKET_MONTH
)

,MultipleTicketsMasterTable AS(
  SELECT f.*,ContratoTicket AS MultipleTickets
  FROM TwoTicketsMasterTable f LEFT JOIN MultipleTickets ON ContratoTicket=Fixed_Account AND Month=Ticket_Month
)

----------------------------------------- Customers With Failed Visits -------------------------

,FailedInstallations AS (
    SELECT DISTINCT DATE_TRUNC('Month',interaction_start_time) AS InstallationMonth, account_id AS ContratoInstallations
    FROM (select * from dup_fix where row_num=1)
    WHERE
        interaction_status IN ('CANCELADA','ANULADA')
        --AND TIPO_ATENCION = "TR" -- con esto marcamos que es un truck roll
    GROUP BY 1,2
)

,FailedInstallationsMasterTable AS(
  SELECT DISTINCT f.*, ContratoInstallations AS FailedInstallations
  FROM MultipleTicketsMasterTable f LEFT JOIN FailedInstallations ON ContratoInstallations=Fixed_Account AND f.Month=InstallationMonth
)

----------------------------------- Tech Tickets ---------------------------

,NumTiquetes AS(
    SELECT account_id AS Contrato, Date_trunc('Month',interaction_start_time) AS TiquetMonth, Count(Distinct interaction_id) AS NumTechTickets
    FROM (select * from dup_fix where row_num=1)
    WHERE 
        interaction_purpose_descrip IN (
    'AVERIAS',
    'SIN SERVICIO INTERNET',
    'INTERRUPCION CONSTANT SERVICIO',
    'EQUIPO INTEGRADO',
    'PROB VELOCIDAD',
    'SIN SEÑAL',
    'OTRO INTERNET',
    'CONTROL REMOTO',
    'SIN SERVICIO TV',
    'PROB STB DIG',
    'SIN SEÑAL UNO/VARIOS CH DVB',
    'PROB CABLE MODEM',
    'MENSAJE ERROR DVB',
    'PROB STB DVB',
    'MENSAJE ERROR',
    'INTERNET/DIGITAL',
    'CALIDAD SEÑAL',
    'TV/INTERNET/DIGITAL',
    'SIN SERVICIO TODOS LOS CH DVB',
    'SIN SERVICIO TELEFONIA',
    'CONTROL REMOTO DVB',
    'AVERIA',
    'PROB STB',
    'SEÑAL LLUVIOSA/RAYAS TV',
    'CONEXION',
    'TV/INTERNET',
    'FALTAN CH PARRILLA BASICA DVB',
    'NIVELES SEÑAL INCORRECTOS INT.',
    'TV/DIGITAL',
    'SIN SEÑAL UNO/VARIOS CH',
    'SIN SERVICIO INT',
    'PROB ROUTER CT',
    'CALIDAD SEÑAL DVB'
)
    GROUP BY 1,2
)

,NumTiquetesMasterTable AS(
    SELECT F.*,NumTechTickets 
    FROM failedinstallationsmastertable f LEFT JOIN NumTiquetes ON Contrato=Final_Account AND TiquetMonth=Month
)


---------------------------------------- CSV File -------------------------------
select distinct Month,--B_FinalTechFlag, B_FMC_Segment,B_FMCType, E_FinalTechFlag, E_FMC_Segment,E_FMCType,final_churn_flag,B_TenureFinalFlag,E_TenureFinalFlag,
 count(distinct fixed_account) as activebase, count(distinct oneCall) as OneCall_Flag,count(distinct TwoCalls) as TwoCalls_Flag,count(distinct MultipleCalls) as MultipleCalls_Flag,
 count(distinct OneTicket) as OneTicket_Flag,count(distinct TwoTickets) as TwoTickets_Flag,count(distinct MultipleTickets) as MultipleTickets_Flag,
 count(distinct FailedInstallations) as FailedInstallations_Flag, round(sum(NumTechTickets)) as TicketDensity_Flag
from NumTiquetesMasterTable
Where final_churn_flag<>'Fixed Churner' AND final_churn_flag<>'Customer Gap' AND final_churn_flag<>'Full Churner' AND final_churn_flag<>'Churn Exception'
Group by 1--,2,3,4,5,6,7,8,9,10
Order by 1 desc, 2,3,4
