# Databricks notebook source
# MAGIC %sql 
# MAGIC create database if not exists hf1_demo
# MAGIC location '/mnt/btc1dl/harrisdldemo'

# COMMAND ----------

result_df = spark.read\
.option("inferSchema", True)\
.json("/mnt/btc1dl/harrisdl/2021-03-28/results.json")

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").saveAsTable("hf1_demo.results_managed")  

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_demo.results_managed

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").save("/mnt/btc1dl/harrisdldemo/results_external")   

# COMMAND ----------

# MAGIC %sql
# MAGIC create table hf1_demo.results_external
# MAGIC using DELTA
# MAGIC location '/mnt/btc1dl/harrisdldemo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_demo.results_external

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/btc1dl/harrisdldemo/results_external")

# COMMAND ----------

 display(results_external_df)

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("hf1_demo.results_partitioned")  

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions hf1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md 
# MAGIC - Update Delta Table
# MAGIC - Delete Form Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC update hf1_demo.results_managed 
# MAGIC set points = 11 - position
# MAGIC where position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/btc1dl/harrisdldemo/results_managed")

deltaTable.update("position <= 10", {"points": "21 - position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from hf1_demo.results_managed
# MAGIC where position >10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/btc1dl/harrisdldemo/results_managed")

deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_demo.results_managed;

# COMMAND ----------

# MAGIC %md 
# MAGIC upsert using merge

# COMMAND ----------

drivers_day1_df = spark.read \
.option("inferSchema" , True)\
.json("/mnt/btc1dl/harrisdl/2021-03-28/drivers.json")\
.filter("driverId <= 10")\
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("driver_day1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from driver_day1

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
.option("inferSchema" , True)\
.json("/mnt/btc1dl/harrisdl/2021-03-28/drivers.json")\
.filter("driverId BETWEEN 6 and 15")\
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.schema.names

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("driver_day2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from driver_day2

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
.option("inferSchema" , True)\
.json("/mnt/btc1dl/harrisdl/2021-03-28/drivers.json")\
.filter("driverId BETWEEN 6 and 15 or driverId BETWEEN 16 and 20")\
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists hf1_demo.drivers_merge(
# MAGIC driverId int,
# MAGIC dob DATE,
# MAGIC forename String,
# MAGIC surname string,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC using DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO hf1_demo.drivers_merge tgt
# MAGIC USING driver_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = CURRENT_TIMESTAMP
# MAGIC              
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId , dob , forename , surname , createdDate ) VALUES(driverId , dob , forename, surname , CURRENT_TIMESTAMP)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md 
# MAGIC day2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from driver_day2

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO hf1_demo.drivers_merge tgt
# MAGIC USING driver_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId , dob , forename , surname , createdDate ) VALUES(driverId , dob , forename, surname , CURRENT_TIMESTAMP)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_demo.drivers_merge

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTablePeople = DeltaTable.forPath(spark, '/mnt/btc1dl/harrisdldemo/drivers_merge')


deltaTablePeople.alias('tgt') \
  .merge(
    drivers_day3_df.alias('upd'),
    'tgt.driverId = upd.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "dob" : "upd.dob" , "forename" : "upd.forename" , "surname": "upd.surname", "updatedDate" : "current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "driverId" : "upd.driverId",
        "dob":"upd.dob",
        "forename" : "upd.forename",
        "surname" : "upd.surname",
        "createdDate" : "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history hf1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc History hf1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_demo.drivers_merge version as of 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_demo.drivers_merge TIMESTAMP as of '2022-04-26T11:06:06.000+0000';

# COMMAND ----------

df =  spark.read.format("delta").option("timestampAsOf" , '2022-04-26T11:06:06.000+0000').load("/mnt/btc1dl/harrisdldemo/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum hf1_demo.drivers_merge
# MAGIC --vaccum retains data for 7 days by default

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_demo.drivers_merge TIMESTAMP as of '2022-04-26T11:06:06.000+0000';

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.rententionDurationCheck.enabled = false;
# MAGIC vacuum hf1_demo.drivers_merge retain 0 hours 
# MAGIC --for imidiate deletion

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC vacuum hf1_demo.drivers_merge retain 0 hours 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_demo.drivers_merge version as of 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc History hf1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from hf1_demo.drivers_merge where driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_demo.drivers_merge version as of 6 ;

# COMMAND ----------

# MAGIC %sql 
# MAGIC merge into hf1_demo.drivers_merge tgt
# MAGIC using hf1_demo.drivers_merge version as of 6 src 
# MAGIC     on (tgt.driverId = src.DriverId)
# MAGIC when not matched then
# MAGIC   insert * 

# COMMAND ----------

# MAGIC %sql
# MAGIC desc HISTORY hf1_demo.drivers_merge 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists hf1_demo.drivers_txn(
# MAGIC driverId int,
# MAGIC dob DATE,
# MAGIC forename String,
# MAGIC surname string,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC using DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history hf1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into hf1_demo.drivers_txn
# MAGIC select * from hf1_demo.drivers_merge
# MAGIC where driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history hf1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into hf1_demo.drivers_txn
# MAGIC select * from hf1_demo.drivers_merge
# MAGIC where driverId = 2;

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete  from hf1_demo.drivers_txn
# MAGIC 
# MAGIC where driverId = 1;

# COMMAND ----------

# MAGIC  %sql 
# MAGIC insert into hf1_demo.drivers_txn
# MAGIC select * from hf1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet to delta

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists hf1_demo.drivers_convert_to_delta(
# MAGIC driverId int,
# MAGIC dob DATE,
# MAGIC forename String,
# MAGIC surname string,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC using parquet

# COMMAND ----------

# MAGIC %sql drop table hf1_demo.drivers_convert_to_delta

# COMMAND ----------

# MAGIC %fs ls /mnt/btc1dl/harrisdldemo

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into hf1_demo.drivers_convert_to_delta
# MAGIC select * from hf1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql convert to delta hf1_demo.drivers_convert_to_delta

# COMMAND ----------

df = spark.table("hf1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/btc1dl/harrisdldemo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta PARQUET `/mnt/btc1dl/harrisdldemo/drivers_convert_to_delta_new`