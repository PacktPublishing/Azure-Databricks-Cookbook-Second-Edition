from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerDataAggregation").getOrCreate()

# Path to the CSV file
path_to_csv = "/mnt/data/part-00000-tid-3200334632332214470-9b4dec79-7e2e-495d-8657-3b5457ed3753-108-1-c000.csv"

# Define the schema
schema = "customer_id INT, name STRING, age INT, email STRING, customer_status STRING"

# Set up Auto Loader
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("path", path_to_csv)
      .option("header", "true")
      .schema(schema)
      .load())

# Create a table using the DataFrame
df.writeStream.format("delta").option("checkpointLocation", "/mnt/data/checkpoints").table("customers")

# Load the Auto Loader table as a DataFrame
customers_df = spark.table("customers")

# Perform aggregation operations
aggregated_df = (customers_df.groupBy("customer_status")
                .agg(count("*").alias("total_customers"),
                     avg("age").alias("average_age")))

# Show the results
aggregated_df.show()

# Find the oldest customer per status
oldest_customer_df = (customers_df.groupBy("customer_status")
                      .agg(max("age").alias("max_age"))
                      .join(customers_df, ["customer_status"], "inner")
                      .select("customer_status", "name", "age")
                      .filter(col("age") == col("max_age")))

# Show the oldest customers per status
oldest_customer_df.show()

# Optionally, save the results for further analysis or reporting
aggregated_df.write.format("delta").mode("overwrite").save("/mnt/data/aggregated_customer_data")
