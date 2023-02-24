# Databricks notebook source
# MAGIC %md #### Produce Driver Standings

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

# MAGIC  %run "../include/configuration1"

# COMMAND ----------

# MAGIC %run "../include/Common_functions"

# COMMAND ----------

# race_results_list = spark.read.parquet(f"{presentation_folder_path}/race_results")\
# .filter(f"file_date  = '{v_file_date}'")\
# .select("race_year")\
# .distinct()\
# .collect()


race_results_list = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
.filter(f"file_date  = '{v_file_date}'")\
.select("race_year")\
.distinct()\
.collect()

# COMMAND ----------

race_results_list 

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

print(race_year_list)

# COMMAND ----------

# display(driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum,when,count,col

driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality", "team_name")\
.agg(sum("points").alias("total_points"),
    count(when(col("position") == 1 , True)).alias("wins"))

# COMMAND ----------

 from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

#display(driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings") 

#final_df.write.mode("overwrite").format("parquet").saveAsTable("hf1_presentation.driver_standings") 

# COMMAND ----------

# overwrite_partition(final_df,'hf1_presentation','driver_standings','race_year')
merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'hf1_presentation', 'driver_standings', presentation_folder_path, merge_condition, 'race_year')


# COMMAND ----------

# %sql 
# drop table hf1_presentation.driver_standings

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_presentation.driver_standings where race_year = 2018

# COMMAND ----------

