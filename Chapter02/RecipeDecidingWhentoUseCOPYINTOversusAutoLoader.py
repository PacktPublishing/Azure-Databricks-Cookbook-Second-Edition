from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when, initcap

# Initialize Spark session
spark = SparkSession.builder.appName("IncrementalCustomerDataLoad").getOrCreate()

# Directory path where new CSV files are stored
path_to_csv_dir = "/mnt/data/csv_files/"

# Schema definition
schema = "customer_id INT, name STRING, age INT, email STRING, customer_status STRING"

# Auto Loader setup to ingest CSV data incrementally
customers_df = (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("cloudFiles.useIncrementalListing", "true")
                .option("path", path_to_csv_dir)
                .option("header", "true")
                .schema(schema)
                .load())

# Define a table using the DataFrame for easier SQL operations
customers_df.writeStream.format("delta").option("checkpointLocation", "/mnt/checkpoints/customers").table("customers")

# Applying transformations
transformed_df = (customers_df
                  .withColumn("email", lower(col("email")))
                  .withColumn("age_group",
                              when(col("age") < 18, "Under 18")
                              .when((col("age") >= 18) & (col("age") <= 35), "18-35")
                              .when((col("age") > 35) & (col("age") <= 65), "36-65")
                              .otherwise("Above 65"))
                  .withColumn("name", initcap(col("name"))))

# Writing the transformed data to a Delta table continuously
query = (transformed_df
         .writeStream
         .format("delta")
         .option("checkpointLocation", "/mnt/checkpoints/transformed_customers")
         .outputMode("append")
         .start("/mnt/data/transformed_customers"))

# Wait for the streaming to finish (indefinitely in production)
query.awaitTermination()
