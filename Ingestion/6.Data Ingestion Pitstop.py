# Databricks notebook source
pits_schema = "raceId INT, driverId INT, stop STRING,lap INT,time STRING,duration STRING,milliseconds INT "

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType , DateType

# COMMAND ----------

PitStop_df = spark.read \
.schema(pits_schema)\
.option("multiLine", True)\
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

display(PitStop_df)

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,lit

# COMMAND ----------

final_df = PitStop_df.withColumnRenamed("driverid","driver_id")\
.withColumnRenamed("raceId","race_id")\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("Data_source", lit(v_data_source))
display(final_df)

# COMMAND ----------

#overwrite_partition(final_df,'hf1_processed','PitStop','race_id')

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'hf1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

 dbutils.notebook.exit("success")

# COMMAND ----------

#final_df.write.mode("overwrite").parquet("/mnt/btc1dl/harrisdlprocessed/Pit_Stop")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("hf1_processed.Pit_Stop")
