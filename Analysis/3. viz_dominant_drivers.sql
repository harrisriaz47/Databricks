-- Databricks notebook source
-- MAGIC %python 
-- MAGIC html = """<h1 style="color:Balck;text-align: center; font-family:arial">Report on Dominant Formula 1 drivers</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

select driver_name, 
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
rank() over(order by avg(calculated_points) desc) driver_rank
from hf1_presentation.calculated_race_Results
group by driver_name
having count(1) >= 50
order by avg_points desc 

-- COMMAND ----------

create or replace view v_dominant_driver
as 
select driver_name, 
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
rank() over(order by avg(calculated_points) desc) driver_rank
from hf1_presentation.calculated_race_Results
group by driver_name
having count(1) >= 50
order by avg_points desc 

-- COMMAND ----------

 

select race_year,
driver_name, 
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from hf1_presentation.calculated_race_Results
where driver_name in ( select driver_name from v_dominant_driver where driver_rank <= 10)
group by race_year,driver_name
order by race_year,avg_points desc   

-- COMMAND ----------

 

select race_year,
driver_name, 
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from hf1_presentation.calculated_race_Results
where driver_name in ( select driver_name from v_dominant_driver where driver_rank <= 10)
group by race_year,driver_name
order by race_year,avg_points desc   

-- COMMAND ----------

 

select race_year,
driver_name, 
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from hf1_presentation.calculated_race_Results
where driver_name in ( select driver_name from v_dominant_driver where driver_rank <= 10)
group by race_year,driver_name
order by race_year,avg_points desc   

-- COMMAND ----------

