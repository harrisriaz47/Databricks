# Databricks notebook source
# MAGIC %md Ingest drivers.json file

# COMMAND ----------

# MAGIC   %run "../include/configuration1"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

# MAGIC %md
# MAGIC Step! - Read the JSOn file using the spark Dataframe reader API

# COMMAND ----------

# MAGIC %md Step2 - Rename columns and add new column 

# COMMAND ----------

# MAGIC %md 
# MAGIC Step 3 - Drop the unwanted columns
# MAGIC 1. name.forename
# MAGIC 2. name.surname
# MAGIC 3. url

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType , DateType

# COMMAND ----------

 name_schema = StructType(fields=[StructField("forename", StringType(), True),StructField("surname", StringType(), True)
                                                                                     ])

# COMMAND ----------

drivers_schema = StructType(fields=[
StructField("driverId", IntegerType(), False),
StructField("driverRef", StringType(), True), StructField("number", IntegerType(), True),
StructField("code", StringType(), True),
StructField("name", name_schema),
StructField("dob", DateType(), True),
StructField("nationality", StringType(), True),
StructField("url", StringType(), True)
])

# COMMAND ----------

driver_df = spark.read \
.schema(drivers_schema)\
.json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

driver_df.printSchema()

# COMMAND ----------

display(driver_df)

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp, lit

# COMMAND ----------

drives_with_column_df = driver_df.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("driverRef", "driver_ref")\
.withColumn("ingestion_date", current_timestamp())\
.withColumn("name", concat(col("name.forename"),lit(" "), col("name.surname")))

# COMMAND ----------

driver_final_df = drives_with_column_df.drop(col("url"))

# COMMAND ----------

driver_final_df.write.mode("overwrite").parquet("{processed_folder_path}/drivers")


# COMMAND ----------

