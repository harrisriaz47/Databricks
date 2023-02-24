# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingestion constructor.json

# COMMAND ----------

# MAGIC %run "../include/configuration"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
.schema(constructors_schema)\
.json("/mnt/btc1dl/harrisdl/constructors.json")

# COMMAND ----------

 display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Dropping column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_dropped_df = constructors_df.drop(col('url'))

# COMMAND ----------

display(constructor_dropped_df)

# COMMAND ----------

# MAGIC %md Renaming column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId" , "constructor_id")\
.withColumnRenamed("constructorRef" , "construct_ref")\
.withColumn("ingestion_date" , current_timestamp() )

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/btc1dl/harrisdlprocessed/constructors")

# COMMAND ----------

