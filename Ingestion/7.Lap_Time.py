# Databricks notebook source
lap_times_schema = "raceId INT, driverId INT , lap INT, position INT, time STRING, milliseconds STRING"

# COMMAND ----------

# MAGIC %run "../include/Common_functions"

# COMMAND ----------

# MAGIC %run "../include/configuration1"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,lit

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverid","driver_id")\
.withColumnRenamed("raceId","race_id")\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("Data_source", lit(v_data_source))

display(final_df)

# COMMAND ----------

# final_df.write.mode("overwrite").parquet("/mnt/btc1dl/harrisdlprocessed/lap_Times")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("hf1_processed.lap_times")

# COMMAND ----------

#overwrite_partition(final_df,'hf1_processed','Lap_times','race_id')

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'hf1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table hf1_processed.lap_times

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_Id , count(1) from hf1_processed.results
# MAGIC group by race_Id
# MAGIC order by race_id desc;

# COMMAND ----------

