# %%
from pyspark.sql import SparkSession

# %%
user = "sa"
password  = "<yousry-password>"

# %%
spark = (
    SparkSession.builder.master("local[*]")
    .appName("jdbc-push-down")
    .config('spark.jars.packages', 'com.microsoft.sqlserver:mssql-jdbc:12.4.0.jre11')
    .config("spark.driver.memory", "12g")
    .getOrCreate()
)

spark.version

# %%
records = [
    (1,"Northwest"),
    (2,"Northeast"),
    (3,"Central"),
    (4,"Southwest"),
    (5,"Southeast"),
    (6,"Canada"),
    (7,"France"),
    (8,"Germany"),
    (9,"Australia"),
    (10,"United Kingdom")
]

df_regions = spark.createDataFrame(records, schema="TerritoryID:int, Name:string")
df_regions.show()

# %%
database = "AdventureWorks2022"
table = "Sales.SalesOrderHeader"

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

df_groups = df.join(df_regions, "TerritoryID").where("OnlineOrderFlag = 0").groupBy("Name").count()

df_groups.show()

# %%



