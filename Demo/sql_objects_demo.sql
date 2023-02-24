-- Databricks notebook source
create database Harrisdemo ;

-- COMMAND ----------

create database if not exists Harrisdemo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

desc database harrisdemo;

-- COMMAND ----------

desc database extended Harrisdemo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

show tables IN Harrisdemo;

-- COMMAND ----------

use demo;

-- COMMAND ----------


select current_database();

-- COMMAND ----------


show tables;

-- COMMAND ----------

show tables in default;

-- COMMAND ----------

-- MAGIC %md #### Managed Tables

-- COMMAND ----------

-- MAGIC    %run "../include/configuration1"

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("Harrisdemo.race_result_python")

-- COMMAND ----------

use Harrisdemo;
SHow tables;

-- COMMAND ----------

desc extended race_result_python;

-- COMMAND ----------


select * from demo.race_result_python where race_year = 2020;

-- COMMAND ----------

create table Harrisdemo.race_results_sql
as
select * from Harrisdemo.race_result_python where race_year = 2020;

-- COMMAND ----------

desc extended Harrisdemo.race_results_sql;

-- COMMAND ----------

drop table Harrisdemo.race_results_sql;

-- COMMAND ----------

show tables in Harrisdemo;

-- COMMAND ----------

-- MAGIC %md ####Extended Tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py")\
-- MAGIC .saveAsTable("harrisdemo.race_results_ext_py")

-- COMMAND ----------

desc extended Harrisdemo.race_results_ext_py

-- COMMAND ----------

drop table Harrisdemo.race_result_ext_sql;

-- COMMAND ----------

create table Harrisdemo.race_results_ext_sql(
race_year int,
race_name string,
circuit_location string,
driver_name string,
team string,
driver_number int,
driver_nationality string,
grid int,
fastest_lap int,
race_time string,
points float,
position int,
created_date timestamp
)

using parquet
location "/mnt/btc1dl/harrisdlpresentation/race_result_ext_sql "

-- COMMAND ----------


insert into Harrisdemo.race_results_ext_sql
select * from Harrisdemo.race_results_ext_py where race_year = 2020;

-- COMMAND ----------

select count(1) from Harrisdemo.race_results_ext_sql;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Veiws on tables using sql in databricks

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use Harrisdemo;

-- COMMAND ----------

select current_database()

-- COMMAND ----------



-- COMMAND ----------

create or replace temp view v_race_results
as
select * from Harrisdemo.race_result_python
where race_year = 2018;

-- COMMAND ----------

select * from v_race_results

-- COMMAND ----------

create or replace global temp view gb_race_results
as
select * from Harrisdemo.race_result_python
where race_year = 2012;

-- COMMAND ----------

select * from global_temp.gb_race_results

-- COMMAND ----------

create or replace view Harrisdemo.pv_race_results
as
select * from Harrisdemo.race_result_python
where race_year = 2000;

-- COMMAND ----------

show tables in Harrisdemo

-- COMMAND ----------

