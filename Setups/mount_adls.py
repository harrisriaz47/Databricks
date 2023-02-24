# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("harris-scope")

# COMMAND ----------

storage_account_name = "btc1dl"
client_id = dbutils.secrets.get(scope="harris-scope", key="harrisclientid")
tenant_id = dbutils.secrets.get(scope="harris-scope", key="harristenantid")
client_secret = dbutils.secrets.get(scope="harris-scope", key="harrisclientSC")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": f"{client_id}",
       "fs.azure.account.oauth2.client.secret": f"{client_secret}",
       "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

# COMMAND ----------

# MAGIC %md
# MAGIC Mounting databricks with azure data lake container

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_adls("harrisdldemo")

# COMMAND ----------

mount_adls("harrisdl")

# COMMAND ----------

mount_adls("harrisdlprocessed")

# COMMAND ----------

mount_adls("harrisdlpresentation")

# COMMAND ----------

dbutils.fs.ls("/mnt/btc1dl/harrisdl")

# COMMAND ----------

dbutils.fs.ls("/mnt/btc1dl/harrisdlprocessed")

# COMMAND ----------

dbutils.fs.ls("/mnt/btc1dl/harrisdlpresentation")

# COMMAND ----------

dbutils.fs.ls("/mnt/btc1dl/harrisdldemo")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

#dbutils.fs.unmount("/mnt/btc1dl/")

# COMMAND ----------

#dbutils.fs.unmount("/mnt/btc1dl/")

# COMMAND ----------

