-- Databricks notebook source
select driver_name, 
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from hf1_presentation.calculated_race_Results
group by driver_name
having count(1) >= 50
order by avg_points desc 

-- COMMAND ----------

 select driver_name, 
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from hf1_presentation.calculated_race_Results
where race_year between 2001 and 2010
group by driver_name
having count(1) >= 50
order by avg_points desc 

-- COMMAND ----------

