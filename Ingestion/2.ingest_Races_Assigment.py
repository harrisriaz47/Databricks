# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType , DateType 


# COMMAND ----------

# MAGIC %run "../include/Common_functions"

# COMMAND ----------

# MAGIC %run "../include/configuration1"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

Race_schema = StructType(fields =[

StructField("raceid" , IntegerType(),True),
StructField("year" , IntegerType(),True),
StructField("round" , IntegerType(),True),
StructField("circuitId" , IntegerType(),True),
StructField("name" , StringType(),True),
StructField("date" , DateType(),True),
StructField("time" , StringType(),True),
StructField("url" , StringType(),True)]) 

# COMMAND ----------

 races_df =   spark.read.option("header",True)\
.schema(Race_schema)\
 .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add Ingestiion date and racetimestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col , lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp())\
.withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))\
.withColumn("Data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col("raceid").alias("race_id"), col("year").alias("race_year"), col("round"),col("circuitID").alias("Circuit_id"),col("name"),col("ingestion_date"),col("race_timestamp"),
                                         )

# COMMAND ----------

#races_selected_df.write.mode("overwrite").parquet("/mnt/btc1dl/harrisdlprocessed/races")
#races_selected_df.write.mode("overwrite").format("parquet").saveAsTable("hf1_processed.races")
races_selected_df.write.mode("overwrite").format("delta").saveAsTable("hf1_processed.races")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from hf1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

