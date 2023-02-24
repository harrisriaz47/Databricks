# Databricks notebook source
# MAGIC  %run "../include/configuration1"

# COMMAND ----------

# MAGIC %run "../include/Common_functions"

# COMMAND ----------

# MAGIC %fs ls "dbfs:/mnt/btc1dl/harrisdlprocessed"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
.filter(f"file_date  = '{v_file_date}'")

# COMMAND ----------

race_results_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
.filter(col("race_year").isin(race_results_list))

# COMMAND ----------

from pyspark.sql.functions import sum, when,col,count

Constructor_standings_df = race_results_df \
.groupBy( "race_year" , "team_name") \
.agg(sum("points").alias("total_points"), 
    count(when(col("position") == 1, True)).alias("wins"))
 

# COMMAND ----------

display(Constructor_standings_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = Constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("hf1_presentation.constructor_standings")

# COMMAND ----------

# overwrite_partition(final_df,'hf1_presentation','constructor_standings','race_year')



# COMMAND ----------

merge_condition = "tgt.team_name = src.team_name AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'hf1_presentation', 'constructor_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_presentation.constructor_standings where race_year = 2018 order by rank;

# COMMAND ----------

