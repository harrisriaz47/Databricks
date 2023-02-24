# Databricks notebook source
qualifying_schema = "qualifyId INT, raceId INT ,driverId  INT, constructorId INT, number STRING, position STRING , q1 STRING, q2 STRING, q3 STRING"

# COMMAND ----------

dbutils.widgets.text("p_file_date"," ")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
var_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../include/configuration1"

# COMMAND ----------

# MAGIC %run "../include/Common_functions"

# COMMAND ----------

qualifying_df = spark.read\
.schema(qualifying_schema)\
.option("multiLine", True)\
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyid","qualify_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("constructorId","constructor_id")\
.withColumn("ingestion_date",current_timestamp())
display(final_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table hf1_processed.qualifying

# COMMAND ----------

#final_df.write.mode("overwrite").parquet("/mnt/btc1dl/harrisdlprocessed/qualifying")
#final_df.write.mode("overwrite").format("parquet").saveAsTable("hf1_processed.qualifying")

# COMMAND ----------

#overwrite_partition(final_df,'hf1_processed','qualifying','race_id')

merge_condition = "tgt.qualify_id = src.qualify_id  AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'hf1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

