# Databricks notebook source


# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("harris-scope")

# COMMAND ----------

dbutils.secrets.get(scope = "harris-scope", key= "harrisclientid")

# COMMAND ----------



# COMMAND ----------

storage_account_name = "btc1dl"
client_id = dbutils.secrets.get(scope = "harris-scope", key= "harrisclientid")
client_secret= dbutils.secrets.get(scope = "harris-scope", key= "harrisclientSC")
tenant_id = dbutils.secrets.get(scope = "harris-scope", key= "harristenantid")

# COMMAND ----------



# COMMAND ----------


configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": f"{client_id}",
       "fs.azure.account.oauth2.client.secret": f"{client_secret}",
       "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)

# COMMAND ----------

mount_adls("harrisdlprocessed")

# COMMAND ----------

dbutils.fs.ls("/mnt/btc1dl/harrisdl")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount(
source = f"abfss://procssed@{storage_account_name}.dfs.core.windows.net/",
mount_point = f"/mnt/{storage_account_name}/processed",
extra_configs = configs)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.fs.unmount("/mnt/btc1dl/harrisdlprocessed")

# COMMAND ----------

dbutils.fs.unmount("/mnt/btc1dl/raw")

# COMMAND ----------

