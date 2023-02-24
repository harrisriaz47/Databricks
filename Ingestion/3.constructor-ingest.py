# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest constructor.json file

# COMMAND ----------

# MAGIC 
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

#defining schema for JSON file
constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

#reading JSON file
constructor_df = spark.read \
.schema(constructor_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

#check Schema
display(constructor_df)

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Dropping A column

# COMMAND ----------

#Method - 1
#dropping a column
constructor_drop_df = constructor_df.drop('url')


# COMMAND ----------

#Method - 2
#dropping a column
constructor_drop_df = constructor_df.drop(constructor_df['url'])


# COMMAND ----------

#Method - 3
#dropping a column

from pyspark.sql.functions import col
constructor_drop_df = constructor_df.drop(col('url'))
display(constructor_drop_df)


# COMMAND ----------

# MAGIC   %md
# MAGIC   ### Rename Columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

constructor_final_df = constructor_drop_df.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("constructorRef", "constructor_ref") \
.withColumnRenamed("name", "team_name") \
.withColumn("ingestiion_date", current_timestamp())\
.withColumn("Data_source", lit(v_data_source))\
.withColumn("Data_source", lit(v_file_date))

display(constructor_final_df)



# COMMAND ----------

# MAGIC %md
# MAGIC ###Write to parquet file

# COMMAND ----------

#constructor_final_df.write.mode("overwrite").parquet("/mnt/btc1dl/harrisdlprocessed/contructors")
#constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("hf1_processed.constructors")
constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("hf1_processed.constructors")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from hf1_processed.constructors

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

