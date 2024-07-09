#Run in python
dbutils.fs.mounts()

#Run in python
dbutils.fs.ls("/mnt/your-mount-name")

#Run in Spark
file_path = "/mnt/your-mount-name/path/to/your-file.csv"
df = spark.read.format("csv").option("header", "true").
load(file_path)
df.show()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg,
max
# Create or get the Spark session
spark = SparkSession.builder.
appName("CustomerDataAggregation").getOrCreate()
# Path where the uploaded CSV file is stored
path_to_csv = "/mnt/data/part-00000-tid3200334632332214470-9b4dec79-7e2e-495d-8657-
3b5457ed3753-108-1-c000.csv"
# Define the schema based on the known structure of
the CSV
schema = "customer_id INT, name STRING, age INT,
email STRING, customer_status STRING"
# Set up the Auto Loader
df = (spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "csv")
 .option("path", path_to_csv)
 .option("header", "true")
 .schema(schema)
 .load())
# Create a table using the DataFrame
df.writeStream.format("delta").
option("checkpointLocation", "/mnt/data/
checkpoints").table("customers")
# Load the Auto Loader table as a DataFrame
customers_df = spark.table("customers")
# Perform aggregation operations
aggregated_df = (customers_df.groupBy("customer_
status")
 .agg(count("*").alias("total_customers"),
 avg("age").alias("average_age")))
# Show the results
aggregated_df.show()
# Find the oldest customer per status
oldest_customer_df = (customers_df.groupBy("customer_
status")
 .agg(max("age").alias("max_age"))
 .join(customers_df, ["customer_status"], "inner")
 .select("customer_status", "name", "age")
 .filter(col("age") == col("max_age")))
# Show the oldest customers per status
oldest_customer_df.show()
# Optionally, save the results for further analysis
or reporting
aggregated_df.write.format("delta").
mode("overwrite").save("/mnt/data/aggregated_
customer_data")


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper
# Initialize Spark session
spark = SparkSession.builder.
appName("CustomerDataTransformation").getOrCreate()
# Define the path to the uploaded CSV file
path_to_csv = "/mnt/data/part-00000-tid3200334632332214470-9b4dec79-7e2e-495d-8657-3b5457ed3753-
108-1-c000.csv"
# Auto Loader setup to ingest CSV data
customers_df = (spark.readStream.format("cloudFiles")
 .option("cloudFiles.format", "csv")
 .option("path", path_to_csv)
 .option("header", "true")
 .load())
# Define a table using the DataFrame for easier SQL
operations
customers_df.writeStream.format("delta").
option("checkpointLocation", "/mnt/checkpoints").
table("customers")

# Load the Auto Loader table as a DataFrame
customers_df = spark.table("customers")
# Transformation 1: Standardize email addresses
customers_df = customers_df.withColumn("email",
lower(col("email")))
# Transformation 2: Categorize into age groups
customers_df = customers_df.withColumn("age_group",
 when(col("age") < 18, "Under 18")
 .when((col("age") >= 18) & (col("age") <= 35),
"18-35")
 .when((col("age") > 35) & (col("age") <= 65),
"36-65")
 .otherwise("Above 65"))
# Transformation 3: Capitalize names
customers_df = customers_df.withColumn("name",
initcap(col("name")))
# Display the transformed DataFrame
customers_df.show()
# Optionally, save the transformed data for further
analysis
customers_df.write.format("delta").mode("overwrite").
save("/mnt/data/transformed_customers")


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when,
initcap
# Initialize Spark session
spark = SparkSession.builder.
appName("IncrementalCustomerDataLoad").getOrCreate()
# Directory path where new CSV files are stored
path_to_csv_dir = "/mnt/data/csv_files/"


# Schema definition (optional, remove if you prefer
schema inference)
schema = "customer_id INT, name STRING, age INT,
email STRING, customer_status STRING"
# Auto Loader setup to ingest CSV data incrementally
customers_df = (spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "csv")
 .option("cloudFiles.useIncrementalListing", "true") # Enable incremental file processing
 .option("path", path_to_csv_dir)
 .option("header", "true")
 .schema(schema) # Comment this if schema
inference is preferred
 .load())
# Define a table using the DataFrame for easier SQL
operations
customers_df.writeStream.format("delta").
option("checkpointLocation", "/mnt/checkpoints/
customers").table("customers")

# Applying transformations
transformed_df = (customers_df
 .withColumn("email", lower(col("email")))
 .withColumn("age_group",
 when(col("age") < 18, "Under 18")
 .when((col("age") >= 18) & (col("age") <= 35),
"18-35")
 .when((col("age") > 35) & (col("age") <= 65),
"36-65")
 .otherwise("Above 65"))
 .withColumn("name", initcap(col("name"))))
# Writing the transformed data to a Delta table
continuously
query = (transformed_df
 .writeStream
 .format("delta")
 .option("checkpointLocation", "/mnt/checkpoints/
transformed_customers")
 .outputMode("append")
 .start("/mnt/data/transformed_customers"))
# Wait for the streaming to finish (indefinitely in
production)
query.awaitTermination()


