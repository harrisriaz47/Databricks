# Databricks notebook source
# MAGIC %run "../include/configuration1"

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_result_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results where race_year = 2020

# COMMAND ----------

p_race_year  = 2020

# COMMAND ----------

race_result_2019_df = spark.sql(f"select * from v_race_results WHERE race_year ={p_race_year} ")

# COMMAND ----------

display(race_result_2019_df)

# COMMAND ----------

# MAGIC %md #### Global Temp Veiw

# COMMAND ----------

race_result_df.createOrReplaceGlobalTempView ("gb_race_results")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results

# COMMAND ----------

spark.sql("select * from global_temp.gv_race_results ").show()

# COMMAND ----------

