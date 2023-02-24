-- Databricks notebook source
create database if not exists hf1_raw;


-- COMMAND ----------

-- MAGIC %md create ciruits table

-- COMMAND ----------

drop table if exists hf1_raw.circuits;
create table if not exists hf1_raw.circuits(
circuitId Int,
circuitRef String,
name String,
location String,
country String,
lat Double,
lng Double,
alt Int,
url String
)
using csv
options (path "/mnt/btc1dl/harrisdl/circuits.csv", header true )

-- COMMAND ----------

select * from hf1_raw.circuits

-- COMMAND ----------

-- MAGIC %md #### create races table

-- COMMAND ----------

Drop table if exists hf1_raw.races;
create table if not exists hf1_raw.races(
raceId Int,
year Int,
round Int,
circuitId Int,
name String,
date Date,
time String,
url String
)
using csv
options (path "/mnt/btc1dl/harrisdl/races.csv", header true )

-- COMMAND ----------

select * from hf1_raw.races;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Creating tables for json files

-- COMMAND ----------

-- MAGIC %md create constructors table 
-- MAGIC * Single Line json 
-- MAGIC *  Simple structure

-- COMMAND ----------

drop table if exists hf1_raw.constructors;
create table if not exists hf1_raw.constructors(
constructorId INT,
constructorRef STRING, 
name STRING, 
nationality STRING, 
url STRING
)
using json 
options(path "/mnt/btc1dl/harrisdl/constructors.json")

-- COMMAND ----------

select * from hf1_raw.constructors

-- COMMAND ----------

-- MAGIC %md Create drivers table
-- MAGIC - single line json
-- MAGIC - complex structure

-- COMMAND ----------

drop table if exists hf1_raw.drivers;
create table if not exists hf1_raw.drivers(
driverId INT,
dirverRef STRING,
number INT,
code STRING,
name STRUCT<forename : STRING , surname: STRING>,
dob DATE,
nationality STRiNG,
url STRING
)
using json 
options(path "/mnt/btc1dl/harrisdl/drivers.json")

-- COMMAND ----------

select * from hf1_raw.drivers

-- COMMAND ----------

-- MAGIC %md Create results table
-- MAGIC - single line json
-- MAGIC - complex structure

-- COMMAND ----------

drop table if exists hf1_raw.results;
create table if not exists hf1_raw.results(
resultId INT, 
raceId INT,
driverId INT ,
constructorId INT,
number INT,
grid INT,
position INT ,
positionText STRING,
positionOrder INT,
points FLOAT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
FastestLapTime STRING,
fastestLapSpeed STRING,
statusId INT
)
using json 
options(path "/mnt/btc1dl/harrisdl/results.json")

-- COMMAND ----------

select * from hf1_raw.results

-- COMMAND ----------

-- MAGIC %md create pit stops table 
-- MAGIC - multiline json
-- MAGIC - Simple structure

-- COMMAND ----------

drop table if exists hf1_raw.pit_stops;
create table if not exists hf1_raw.pit_stops(
raceId INT, 
driverId INT, 
stop STRING,
lap INT,
time STRING,
duration STRING,
milliseconds INT
)
using json 
options(path "/mnt/btc1dl/harrisdl/pit_stops.json" , multiLine true)

-- COMMAND ----------

select * from hf1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md create lap times table
-- MAGIC - csv file
-- MAGIC - Multiple files

-- COMMAND ----------

drop table if exists hf1_raw.lap_times;
create table if not exists hf1_raw.lap_times(
resultId INT,
driverId INT , 
lap INT,
position INT,
time STRING,
milliseconds STRING
) using csv
options(path "/mnt/btc1dl/harrisdl/lap_times")

-- COMMAND ----------

select *from hf1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md creating Qualifying Table 
-- MAGIC - Json file
-- MAGIC - Multiline Json
-- MAGIC - Multiple files

-- COMMAND ----------

drop table if exists hf1_raw.qualifying;
create table if not exists hf1_raw.qualifying(
qualifyId INT,
raceId INT ,
driverId  INT,
constructorId INT,
number STRING,
position STRING ,
q1 STRING,
q2 STRING,
q3 STRING
)
using json 
options(path "/mnt/btc1dl/harrisdl/qualifying", multiLine true)

-- COMMAND ----------

select * from hf1_raw.qualifying

-- COMMAND ----------

