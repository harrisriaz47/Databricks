-- Databricks notebook source
-- MAGIC %python 
-- MAGIC html = """<h1 style="color:Balck;text-align: center; font-family:arial">Report on Dominant Formula 1 Teams</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace view v_dominant_team
as 
select team_name, 
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
rank() over(order by avg(calculated_points) desc) team_rank
from hf1_presentation.calculated_race_Results
group by team_name
having count(1) >= 100
order by avg_points desc 

-- COMMAND ----------

select * from v_dominant_driver

-- COMMAND ----------

 
select race_year,
team_name ,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points
from hf1_presentation.calculated_race_Results
where team_name in (select team_name from v_dominant_team where team_rank <= 5)
group by race_year,team_name
order by race_year,avg_points desc  

-- COMMAND ----------

 
select race_year,
team_name ,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points
from hf1_presentation.calculated_race_Results
where team_name in (select team_name from v_dominant_team where team_rank <= 5)
group by race_year,team_name
order by race_year,avg_points desc  

-- COMMAND ----------

