# %%
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# %%
user = "sa"
password  = "<yousr-password-from-secrets-store-or-env>"

# %%
spark = (
    SparkSession.builder.master("local[*]")
    .appName("jdbc-partitioning")
    .config('spark.jars.packages', 'com.microsoft.sqlserver:mssql-jdbc:12.4.0.jre11')
    .config("spark.driver.memory", "12g")
    .getOrCreate()
)

spark.version

# %%
database = "AdventureWorks2022"
table = "Sales.SalesOrderDetail"

df = (
    spark.read.format("jdbc")
    .option("url", f"jdbc:sqlserver://172.20.64.1:1433;databaseName={database};")
    .option("dbtable", table)
    .option("encrypt", "false")
    .option("user", user)
    .option("password", password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)

df.describe().show()

# %%
database = "AdventureWorks2022"
table = "Sales.SalesOrderDetail"
 
#read table data into a spark dataframe
df = (
    spark.read.format("jdbc")
    .option("url", f"jdbc:sqlserver://172.20.64.1:1433;databaseName={database};")
    .option("dbtable", table)
    .option("encrypt", "false")
    .option("user", user)
    .option("password", password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("partitionColumn", "SalesOrderDetailID")
    .option("lowerBound", "1") # Min and max can be extracted "dynamically" in advance from source DB in a quick manner because they are aggregations
    .option("upperBound", "121317")
    .option("numPartitions", spark.sparkContext.defaultParallelism)
    .load()
)

df.describe().show()

# %%



