# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://input@blb2006.blob.core.windows.net",
  mount_point = "/mnt/repro",
  extra_configs = {"fs.azure.account.key.blb2006.blob.core.windows.net":"Q5cucB7iHbmzEb7zxzdU6Y7V1T1/fo9RO5RLaXrlM6SZss00hfvKMKi3y2GYwSirtWMtJHqCE8Tk+ASt3ofitw=="})

# COMMAND ----------

df = spark.read.option("header", False).csv("/mnt/repro/country_data.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

df.count()

# COMMAND ----------

df.repartition(3).write.mode('overwrite').option("header", False).format("csv").save("/mnt/repro/country")

# COMMAND ----------

print("hello world")

# COMMAND ----------

