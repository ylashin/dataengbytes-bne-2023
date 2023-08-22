# Databricks notebook source
spark.conf.set("spark.databricks.io.cache.enabled", "false")

# COMMAND ----------

import time
import pyspark.sql.functions as F
from pyspark.sql.types import LongType

def expensive_calc(x):    
    time.sleep(0.002)
    return x * x

expensive_udf = udf(expensive_calc, LongType()) 

df = (
    spark.sql("SELECT concat('category', CAST(RAND() * 10 AS INT)) as category, id as value FROM RANGE(100000)")
        .withColumn("squared", expensive_udf("value"))
        .withColumn("cubed", F.expr("squared * value"))
)

df.show(5)

# COMMAND ----------

df.groupBy("category").avg("squared").display()

# COMMAND ----------

df.groupBy("category").avg("cubed").display()

# COMMAND ----------

df.write.mode("overwrite").format("delta").save("dbfs:/tmp/temp-data")

# COMMAND ----------

spark.read.format("delta").load("dbfs:/tmp/temp-data").groupBy("category").avg("squared").display()

# COMMAND ----------

spark.read.format("delta").load("dbfs:/tmp/temp-data").groupBy("category").avg("cubed").display()
