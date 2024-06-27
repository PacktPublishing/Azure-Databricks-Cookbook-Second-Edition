from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialize Spark session
spark = SparkSession.builder.appName("DeltaTablePipeline").getOrCreate()

# Load initial data into a DataFrame from the uploaded CSV
initial_data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/data/dimension_customer.csv")

# Write the DataFrame to a Delta table
initial_data.write.format("delta").save("/delta/initial_table")

# Read the Delta table into a DataFrame
delta_table = spark.read.format("delta").load("/delta/initial_table")

# Apply transformations on the DataFrame
transformed_data = delta_table.filter(delta_table["age"] > 18).select("name", "age")

# Write the transformed data back to the Delta table
transformed_data.write.format("delta").mode("overwrite").save("/delta/transformed_table")

# Add a new column to the Delta table
delta_table = delta_table.withColumn("new_column", lit("default_value"))

# Write the updated schema back to the Delta table
delta_table.write.format("delta").mode("overwrite").save("/delta/transformed_table")
