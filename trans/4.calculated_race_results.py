# Databricks notebook source
# %sql
# use hf1_processed;

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE hf1_presentation.calculated_race_results

# COMMAND ----------

spark.sql(f"""
create table if not exists hf1_presentation.calculated_race_results
(
race_year INT,
team_name STRING,
driver_id INT,
driver_name STRING,
race_id INT,
position INT,
points INT,
calculated_points INT,
created_date TIMESTAMP,
updated_date TIMESTAMP
)
using delta
""")

# COMMAND ----------

spark.sql(f"""

        create or replace temp view  race_result_updated
        as
        select races.race_year, 
               constructors.team_name,
               drivers.driver_id,
               drivers.name as driver_name,
               races.race_id,
               results.position,
               results.points,
               11 - results.position as calculated_points
        from hf1_processed.results
        join hf1_processed.drivers on (results.driver_id = drivers.driver_id)
        join hf1_processed.constructors on (results.constructor_id = constructors.constructor_id)
        join hf1_processed.races on (results.race_id = races.race_id)
        where results.position <= 10
        and results.file_date = '{v_file_date}'

""")


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO hf1_presentation.calculated_race_results tgt
# MAGIC USING race_result_updated upd
# MAGIC ON ( tgt.driver_id = upd.driver_id and tgt.race_id = upd.race_id)
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.position = upd.position,
# MAGIC              tgt.points = upd.points,
# MAGIC              tgt.calculated_points = upd.calculated_points,
# MAGIC              tgt.updated_date = CURRENT_TIMESTAMP
# MAGIC              
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, created_date ) VALUES(race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, CURRENT_TIMESTAMP)

# COMMAND ----------

# MAGIC %sql select * from race_result_updated where race_year = 2021;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from race_result_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from hf1_presentation.calculated_race_results; 

# COMMAND ----------

