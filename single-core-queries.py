# Databricks notebook source
spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# disable AQE as data volumn is small and we want to focus on query behaviour
spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")

df = spark.read.format("delta").load("dbfs:/databricks-datasets/nyctaxi-with-zipcodes/subsampled")

window_spec  = Window.partitionBy("pickup_zip").orderBy("fare_amount")
df.withColumn("row_number",F.row_number().over(window_spec)).write.format("noop").mode("overwrite").save()

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

df = spark.read.format("delta").load("dbfs:/databricks-datasets/nyctaxi-with-zipcodes/subsampled")

window_spec  = Window.orderBy("fare_amount")
df.withColumn("row_number",F.row_number().over(window_spec)).write.format("noop").mode("overwrite").save()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sample_table AS
# MAGIC WITH source AS 
# MAGIC (
# MAGIC   SELECT * FROM delta.`dbfs:/databricks-datasets/nyctaxi-with-zipcodes/subsampled`
# MAGIC )
# MAGIC SELECT * , NULL as other_column FROM source

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC sample_table

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH 
# MAGIC q1 AS (
# MAGIC   SELECT ARRAY(0.1f, 0.1f, 0.1f) as numbers
# MAGIC ),
# MAGIC q2 as (
# MAGIC SELECT explode(numbers) as number FROM q1
# MAGIC )
# MAGIC SELECT SUM(number) FROM q2

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH 
# MAGIC q1 AS (
# MAGIC   SELECT ARRAY(0.1d, 0.1d, 0.1d) as numbers
# MAGIC ),
# MAGIC q2 as (
# MAGIC SELECT explode(numbers) as number FROM q1
# MAGIC )
# MAGIC SELECT SUM(number) FROM q2

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH 
# MAGIC q1 AS (
# MAGIC   SELECT ARRAY(0.1BD, 0.1BD, 0.1BD) as numbers
# MAGIC ),
# MAGIC q2 as (
# MAGIC SELECT explode(numbers) as number FROM q1
# MAGIC )
# MAGIC SELECT SUM(number) FROM q2

# COMMAND ----------

1
