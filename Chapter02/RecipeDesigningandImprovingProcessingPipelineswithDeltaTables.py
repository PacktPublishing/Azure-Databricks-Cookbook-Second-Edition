from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Initialize Spark session
spark = SparkSession.builder.appName("DeltaTableProcessing").getOrCreate()

# Step 1: Load initial data from CSV and write to a Delta table
initial_data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/data/dimension_customer.csv")
initial_data.write.format("delta").save("/delta/initial_table")

# Step 2: Apply transformations on the Delta table
delta_table = spark.read.format("delta").load("/delta/initial_table")
transformed_data = delta_table.filter(col("age") > 18).select("name", "age")
transformed_data.write.format("delta").mode("overwrite").save("/delta/transformed_table")

# Step 3: Update and Merge operations
# Perform an update operation on the Delta table
delta_table.update(condition="age < 18", set={"status": "minor"})

# Prepare updated data for merging (hypothetical setup)
updated_data = spark.createDataFrame(...)  # Replace with actual data setup

# Perform a merge operation
delta_table.merge(
    source=updated_data,
    condition="delta_table.id = updated_data.id",
    update={"age": updated_data.age}
)

# Step 4: Schema evolution
delta_table = spark.read.format("delta").load("/delta/transformed_table")
delta_table = delta_table.withColumn("new_column", lit("default_value"))
delta_table.write.format("delta").mode("overwrite").save("/delta/transformed_table")

# Step 5: Data Versioning and Testing
delta_table.createOrReplaceTempView("delta_table")
test_data = spark.sql("SELECT * FROM delta_table WHERE age > 30")
version_1 = spark.read.format("delta").option("versionAsOf", 1).load("/delta/transformed_table")
version_2 = spark.read.format("delta").option("versionAsOf", 2).load("/delta/transformed_table")
