# Databricks notebook source
spark.read.json("/mnt/btc1dl/harrisdl/2021-03-21/results.json").createOrReplaceTempView("results_cutover")

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId , count(1) 
# MAGIC   from results_cutover
# MAGIC   group by raceId
# MAGIC   order by raceId desc;

# COMMAND ----------

spark.read.json("/mnt/btc1dl/harrisdl/2021-04-18/results.json").createOrReplaceTempView("results_w2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId , count(1) 
# MAGIC   from results_w2
# MAGIC   group by raceId
# MAGIC   order by raceId desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId , count(1) 
# MAGIC   from results_w2
# MAGIC   group by raceId
# MAGIC   order by raceId desc;

# COMMAND ----------

results_schema = "resultId INT, raceId INT, driverId INT , constructorId INT, number INT, grid INT, position INT , positionText STRING, positionOrder INT, points FLOAT, laps INT, time STRING, milliseconds INT, fastestLap INT, rank INT, FastestLapTime STRING, fastestLapSpeed STRING,statusId INT"

# COMMAND ----------

# MAGIC %run "../include/Common_functions"

# COMMAND ----------

# MAGIC %run "../include/configuration1"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date"," ")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType , DateType

# COMMAND ----------

results_df = spark.read \
.schema(results_schema)\
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,lit

# COMMAND ----------

results_droped_df = results_df.drop(col("statusId"))

# COMMAND ----------

results_final_df = results_droped_df.withColumnRenamed("resultID" , "result_id")\
.withColumnRenamed("raceId" , "race_id")\
.withColumnRenamed("resultId" , "result_id")\
.withColumnRenamed("driverId" , "driver_id")\
.withColumnRenamed("constructorId" , "constructor_id")\
.withColumnRenamed("positionText" , "position_text")\
.withColumnRenamed("positionOrder" , "position_order")\
.withColumnRenamed("fastestLapTime" , "fastest_Lap_Time")\
.withColumnRenamed("fastestLapSpeed" , "fastest_Lap_Speed")\
.withColumn("Ingestion_date" , current_timestamp())\
.withColumn("Data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))


# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md de-dupe the dataframe

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md Method 1

# COMMAND ----------

# collect makes a list

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("hf1_processed.results")):
#         spark.sql(f"alter table hf1_processed.results drop if exists partition (race_id = {race_id_list.race_id})")
        
# here in every iteration parition of race_id is dropped if it is in list
# removing duplicate rows/partitions

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("hf1_processed.results")

# COMMAND ----------

# MAGIC %md Method 2

# COMMAND ----------

# MAGIC %sql 
# MAGIC --drop table hf1_processed.results

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# results_final_df = results_final_df.select("result_id","driver_id", "constructor_id","number","grid","position","position_text","position_order","points","laps","time","milliseconds","fastestLap","rank","fastest_lap_time","fastest_lap_speed","data_source","file_date","ingestion_date","race_id")
# display(results_final_df)

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists("hf1_processed.results")):
#     results_final_df.write.mode("overwrite").insertInto("hf1_processed.results")
# else:
#     results_final_df.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable("hf1_processed.results")


# COMMAND ----------

output_df = re_arrange_partition_column(results_final_df, 'race_id')

# COMMAND ----------

display(output_df)

# COMMAND ----------

#overwrite_partition(results_final_df,'hf1_processed','results','race_id')

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'hf1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

 dbutils.notebook.exit("success")

# COMMAND ----------

# results_final_df.write.mode("overwrite").insertInto("hf1_processed.results")

# COMMAND ----------

# MAGIC %%sql
# MAGIC select race_Id , count(1) from hf1_processed.results
# MAGIC group by race_Id
# MAGIC order by race_id desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id , driver_id ,count(1) 
# MAGIC from hf1_processed.results
# MAGIC group by race_id, driver_id
# MAGIC having count(1) > 1
# MAGIC order by race_id, driver_id desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_processed.results where race_id = 540 and driver_id = 229;

# COMMAND ----------

