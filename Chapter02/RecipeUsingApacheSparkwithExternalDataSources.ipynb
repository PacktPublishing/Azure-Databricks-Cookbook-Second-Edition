from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder.appName("ExternalDataSource").getOrCreate()

# Define the schema of the CSV file
cust_schema = StructType([
    StructField("C_CUSTKEY", IntegerType()),
    StructField("C_NAME", StringType()),
    StructField("C_ADDRESS", StringType()),
    StructField("C_NATIONKEY", ShortType()),
    StructField("C_PHONE", StringType()),
    StructField("C_ACCTBAL", DoubleType()),
    StructField("C_MKTSEGMENT", StringType()),
    StructField("C_COMMENT", StringType())
])

# Load the CSV files with the specified schema
df_cust_sch = spark.read.format("csv").option("header", True).schema(cust_schema).load("/mnt/adls/Common/Customer/csvFiles")

# Display the DataFrame
df_cust_sch.show()
