# Databricks notebook source
df = spark.sql("SELECT SHA2(CAST(RANDOM() AS STRING), 256) as random_string from RANGE(10000000)").cache()
df.count()
df.show(2, truncate=False)

# COMMAND ----------

import pyspark.sql.functions as F

def extract_first_n_chars(value: str, n: int):    
    return value[0: n]

extract_first_n_chars_udf = udf(extract_first_n_chars) 

grouped_df = (
    df
        .withColumn("first_two", extract_first_n_chars_udf("random_string", F.lit(2)))
        .groupBy("first_two")
        .count()
)

grouped_df.write.format("noop").mode("overwrite").save()

# COMMAND ----------

grouped_df = (
    df
        .withColumn("first_two", F.substring("random_string", 1, 2))
        .groupBy("first_two").count()
)

grouped_df.write.format("noop").mode("overwrite").save()

# COMMAND ----------

import pandas as pd

@F.pandas_udf("string")
def pandas_substring_udf(value: pd.Series) -> pd.Series:
    return value.str.slice(start=0, stop=2)

grouped_df = (
    df
        .withColumn("first_two", pandas_substring_udf("random_string"))
        .groupBy("first_two").count()
)

grouped_df.write.format("noop").mode("overwrite").save()

# COMMAND ----------


