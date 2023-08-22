# Databricks notebook source
df = spark.read.format("delta").load("dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow")
df.limit(40000000).write.mode("overwrite").saveAsTable("trips")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT vendor_id FROM trips

# COMMAND ----------

df_vendors = spark.createDataFrame(
    [("CMT", "CMT Group"), ("VTS", "VTS Transportation Inc"), ("DDS", "DDS Taxis")],
    "vendor_id string, vendor_name string",
)

spark.table("trips").join(df_vendors, "vendor_id").groupBy(
    "vendor_name"
).count().display()

# COMMAND ----------

from pyspark.sql.functions import broadcast
df_vendors = spark.createDataFrame(
    [("CMT", "CMT Group"), ("VTS", "VTS Transportation Inc"), ("DDS", "DDS Taxis")],
    "vendor_id string, vendor_name string",
)

spark.table("trips").join(broadcast(df_vendors), "vendor_id").groupBy(
    "vendor_name"
).count().display()

# COMMAND ----------

df_vendors = spark.createDataFrame(
    [("CMT", "CMT Group"), ("VTS", "VTS Transportation Inc"), ("DDS", "DDS Taxis")],
    "vendor_id string, vendor_name string",
)

df_vendors.write.mode("overwrite").saveAsTable("vendors")

# COMMAND ----------

spark.table("trips").join(spark.table("vendors"), "vendor_id").groupBy(
    "vendor_name"
).count().display()

# COMMAND ----------


