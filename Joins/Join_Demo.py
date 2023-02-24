# Databricks notebook source
# MAGIC %run "../include/configuration1"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuit")\
.filter("circuit_id < 70")\
.withColumnRenamed("name","circuit_name")
display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019")\
.withColumnRenamed("name", "race_name")

# COMMAND ----------


display(races_df)
display(circuits_df)

# COMMAND ----------

# MAGIC %md inner joins

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.Circuit_id, "inner")

# COMMAND ----------

  display(race_circuit_df)

# COMMAND ----------

# MAGIC    %md Outter joins

# COMMAND ----------

#left outer join
race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.Circuit_id, "left")
display(race_circuit_df)

# COMMAND ----------

#Right outer join
race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.Circuit_id, "right")
display(race_circuit_df)

# COMMAND ----------

#full outer join
race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.Circuit_id, "full")
display(race_circuit_df)

# COMMAND ----------

rce_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.Circuit_id, "semi")\
.select(circuits_df.circuit_name,circuits_df.location,circuits_df.mycountry)
display(race_circuit_df)

# COMMAND ----------

race_circuit_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.Circuit_id, "anti")
display(race_circuit_df)

# COMMAND ----------

race_circuits_df = races_df.crossJoin(circuits_df)
display(race_circuits_df)

# COMMAND ----------

