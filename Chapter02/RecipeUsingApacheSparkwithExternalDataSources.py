# Display the files in the specified directory
display(dbutils.fs.ls("/mnt/adls/Common/Customer/csvFiles/"))

# Reading the CSV files with inferred schema
df_cust = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/adls/Common/Customer/csvFiles")

# Importing required types from pyspark.sql.types
from pyspark.sql.types import *

# Defining the schema
cust_schema = StructType([
    StructField("C_CUSTKEY", IntegerType(), True),
    StructField("C_NAME", StringType(), True),
    StructField("C_ADDRESS", StringType(), True),
    StructField("C_NATIONKEY", ShortType(), True),
    StructField("C_PHONE", StringType(), True),
    StructField("C_ACCTBAL", DoubleType(), True),
    StructField("C_MKTSEGMENT", StringType(), True),
    StructField("C_COMMENT", StringType(), True)
])

# Reading the CSV files with the defined schema
df_cust_sch = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(cust_schema) \
    .load("/mnt/adls/Common/Customer/csvFiles")
