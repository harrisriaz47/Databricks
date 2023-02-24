# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date"," ")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

results = dbutils.notebook.run("1.ingest_circuit" , 0 , {"p_data_source":"Ergast API" , "p_file_date":"2021-04-18"})

# COMMAND ----------

results

# COMMAND ----------

results = dbutils.notebook.run("2.ingest_Races_Assigment" , 0 , {"p_data_source":"Ergast API" , "p_file_date":"2021-04-18"})
results

# COMMAND ----------

results = dbutils.notebook.run("3.constructor-ingest" , 0 , {"p_data_source":"Ergast API" , "p_file_date":"2021-04-18"})
results

# COMMAND ----------

results = dbutils.notebook.run("4.ingest_driver" , 0 , {"p_data_source":"Ergast API" , "p_file_date":"2021-04-18"})
results

# COMMAND ----------

results = dbutils.notebook.run("5.ingest_results_assignment " , 0 , {"p_data_source":"Ergast API" , "p_file_date":"2021-04-18"})
results

# COMMAND ----------

results = dbutils.notebook.run("6.Data Ingestion Pitstop" , 0 , {"p_data_source":"Ergast API" , "p_file_date":"2021-04-18"})
results

# COMMAND ----------

results = dbutils.notebook.run("7.Lap_Time" , 0 , {"p_data_source":"Ergast API" , "p_file_date":"2021-04-18"})
results

# COMMAND ----------



# COMMAND ----------

results = dbutils.notebook.run("8.Qualifying_assignment" , 0 , {"p_data_source":"Ergast API" , "p_file_date":"2021-04-18"})
results

# COMMAND ----------

