# Databricks notebook source
# MAGIC %run "../include/Common_functions"

# COMMAND ----------

# MAGIC %run "../include/configuration1"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

v_file_date

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step1:  read the csv using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields =[StructField("circuitId" , IntegerType(),False),

StructField("circuitRef" , StringType(),True),
StructField("name" , StringType(),True),
StructField("location" , StringType(),True),
StructField("country" , StringType(),True),
StructField("lat" , StringType(),True),
StructField("lng" , StringType(),True),
StructField("url" , StringType(),True)]) 

# COMMAND ----------

 circuits_df =   spark.read.option("header",True)\
.option("inferSchema", True)\
.schema(circuits_schema)\
 .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

 display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/btc1dl/harrisdl

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### selecting required column

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId","circuitRef", "name", "location", "country", "lat","lng","url") 

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name,circuits_df.location,circuits_df.country,circuits_df.lat,circuits_df.lng,circuits_df.url)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df["circuitId"],circuits_df["circuitRef"],circuits_df["name"],circuits_df["location"],circuits_df["country"],circuits_df["lat"],circuits_df["lng"],circuits_df["url"])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"),col("location"),col("country").alias("mycountry"),col("lat"),col("lng"),
                                         col("url"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id")\
.withColumnRenamed("circuitRef", "circuit_ref")\
.withColumnRenamed("lat", "lattitude")\
.withColumnRenamed("lng", "longitude")\
.withColumnRenamed("url", "alttitude")


# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Adding ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------


circuits_final_df  = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/btc1dl

# COMMAND ----------

circuits_renamed_df2 = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id")\
.withColumnRenamed("circuitRef", "circuit_ref")\
.withColumnRenamed("lat", "lattitude")\
.withColumnRenamed("lng", "longitude")\
.withColumnRenamed("url", "alttitude")\
.withColumn("Data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

#circuits_renamed_df2.write.mode("overwrite").parquet("dbfs:/mnt/btc1dl/harrisdlprocessed/circuit")
#circuits_renamed_df2.write.mode("overwrite").format("parquet").saveAsTable("hf1_processed.circuits")
circuits_renamed_df2.write.mode("overwrite").format("delta").saveAsTable("hf1_processed.circuits")

# COMMAND ----------


#df = spark.read.parquet("/mnt/btc1dl/harrisdlprocessed/circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hf1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

