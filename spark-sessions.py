# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window

session1 = spark.newSession()
session1.conf.set("spark.sql.shuffle.partitions", "23")
session1.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")

df = session1.read.format("delta").load("dbfs:/databricks-datasets/nyctaxi-with-zipcodes/subsampled")

window_spec  = Window.partitionBy("pickup_zip").orderBy("fare_amount")
df.withColumn("row_number",F.row_number().over(window_spec)).write.format("noop").mode("overwrite").save()

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

session2 = spark.newSession()
session2.conf.set("spark.sql.shuffle.partitions", "55")
session2.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")

df = session2.read.format("delta").load("dbfs:/databricks-datasets/nyctaxi-with-zipcodes/subsampled")

window_spec  = Window.partitionBy("pickup_zip").orderBy("fare_amount")
df.withColumn("row_number",F.row_number().over(window_spec)).write.format("noop").mode("overwrite").save()

# COMMAND ----------


