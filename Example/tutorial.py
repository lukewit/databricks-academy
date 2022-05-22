# Databricks notebook source
dbutils.fs.mount(
source = "wasbs://mycontainer@dbkstutorialstorage.blob.core.windows.net",
mount_point = "/mnt/blobmount",
extra_configs = {"fs.azure.account.key.dbkstutorialstorage.blob.core.windows.net":dbutils.secrets.get(scope = "databricks-tutorial-secret-scope", key = "DbksStorageKey")})

# COMMAND ----------

df = spark.read.text("mnt/blobmount/hw.txt")

# COMMAND ----------

df.show()

# COMMAND ----------

dbutils.fs.unmount("/mnt/blobmount")

# COMMAND ----------

df = spark.read.text("mnt/blobmount/hw.txt")
