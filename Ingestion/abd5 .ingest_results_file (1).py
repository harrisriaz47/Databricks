# Databricks notebook source
# MAGIC %run "../include/configuration1"

# COMMAND ----------

# MAGIC %run "../include/Common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.read.json("/mnt/btc1dl/abd-raw/2021-03-21/results.json").createOrReplaceTempView("results_cutover")

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId, count(1)
# MAGIC from results_cutover
# MAGIC group by raceId
# MAGIC order by raceId desc;

# COMMAND ----------

spark.read.json("/mnt/btc1dl/abd-raw/2021-04-18/results.json").createOrReplaceTempView("results_w2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId, count(1)
# MAGIC from results_w2
# MAGIC group by raceId
# MAGIC order by raceId desc;

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
var_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", StringType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", StringType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapSTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)
                                    
                                ])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("positionText", "position_text") \
.withColumnRenamed("positionOrder", "position_order") \
.withColumnRenamed("fastestLap", "fastest_lap") \
.withColumnRenamed("fastestLapTime", "fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(var_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = results_with_columns_df.drop('statusId')

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# collect makes a list

for race_id_list in results_final_df.select("race_id").distinct().collect():
    if (spark._jsparkSession.catalog().tableExists("hf1_processed.results")):
        spark.sql(f"alter table hf1_processed.results drop if exists partition (race_id = {race_id_list.race_id})")
        
# here in every iteration parition of race_id is dropped if it is in list
# removing duplicate rows/partitions

# COMMAND ----------

results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("hf1_processed.results")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from abd_f1_processed.results
# MAGIC group by race_id

# COMMAND ----------

