# Databricks notebook source
# MAGIC %run "../include/configuration1"

# COMMAND ----------

# MAGIC %run "../include/Common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

# MAGIC %fs ls "dbfs:/mnt/btc1dl/harrisdlprocessed"

# COMMAND ----------

driver_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers")\
.withColumnRenamed("name", "driver_name")\
.withColumnRenamed("number", "driver_number")\
.withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructor_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors")\
.withColumnRenamed("name", "team")
display(constructor_df)

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")\
.withColumnRenamed("location", "circuit_location")
display(circuits_df)

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races")\
.withColumnRenamed("name", "race_name")\
.withColumnRenamed("race_timestamp", "race_date")
display(races_df)

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results")
display(results_df)

# COMMAND ----------

 results_df = spark.read.format("delta").load(f"{processed_folder_path}/results")\
.filter(f"file_date  = '{v_file_date}'")\
.withColumnRenamed("time", "race_time")\
.withColumnRenamed("fastestLap", "fastest_Lap")\
.withColumnRenamed("race_id", "result_race_id")\
.withColumnRenamed("file_date", "result_file_date")
display(results_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC jon circuits to races

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.Circuit_id == circuits_df.circuit_id,"inner")\
.select(races_df.race_id, races_df.race_year, races_df.race_name,circuits_df.circuit_location)
display(race_circuits_df)

# COMMAND ----------

race_result_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id)\
.join(driver_df , results_df.driver_id == driver_df.driver_id)\
.join(constructor_df, results_df.constructor_id == constructor_df.constructor_id)


display(race_result_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_result_df.select("race_id","race_year" , "race_name" , "circuit_location", "driver_name","team_name", "driver_number", "driver_nationality", "grid", "fastest_lap","race_time", "points", "position", "result_file_date")\
.withColumn("created_date", current_timestamp())\
.withColumnRenamed("result_file_date", "file_date")
display(final_df)

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------



# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
#final_df.write.mode("overwrite").format("parquet").saveAsTable("hf1_presentation.race_results") 

# COMMAND ----------

# overwrite_partition(final_df,'hf1_presentation','race_results','race_id')

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'hf1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql 
# MAGIC ---drop hf1_presentation.race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_year from hf1_presentation.race_results group by race_year

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_presentation.race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_Id , count(1) from hf1_processed.results
# MAGIC group by race_Id
# MAGIC order by race_id desc;

# COMMAND ----------

