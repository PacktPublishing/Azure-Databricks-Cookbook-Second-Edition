# Load the initial data into a DataFrame from the
uploaded CSV
initial_data = spark.read.format("csv").option("header",
"true").option("inferSchema", "true").load("/mnt/data/
dimension_customer.csv")
# Write the DataFrame to a Delta table
initial_data.write.format("delta").save("/delta/initial_
table")

# Read the Delta table into a DataFrame
delta_table = spark.read.format("delta").load("/delta/
initial_table")
# Apply transformations on the DataFrame
transformed_data = delta_table.filter(delta_table["age"]
> 18).select("name", "age")
# Write the transformed data back to the Delta table
transformed_data.write.format("delta").mode("overwrite").
save("/delta/transformed_table")


# Perform an update operation on the Delta table
delta_table.update(condition = "age < 18", set =
{"status": "minor"})
# Prepare updated data for merging
updated_data = spark.createDataFrame(...) # hypothetical
updated data setup
# Perform a merge operation
delta_table.merge(
 source = updated_data,
 condition = "delta_table.id = updated_data.id",
 update = {"age": updated_data.age}
)


# Read the transformed Delta table
delta_table = spark.read.format("delta").load("/delta/
transformed_table")
# Add a new column to the Delta table
delta_table = delta_table.withColumn("new_column",
lit("default_value"))
# Write the updated schema back to the Delta table
delta_table.write.format("delta").mode("overwrite").
save("/delta/transformed_table")

# Create a temporary view for the Delta table
delta_table.createOrReplaceTempView("delta_table")
# Extract test data for transformations
test_data = spark.sql("SELECT * FROM delta_table WHERE
age > 30")
# Compare different versions of the data for validation
version_1 = spark.read.format("delta").
option("versionAsOf", 1).load("/delta/transformed_table")
version_2 = spark.read.format("delta").
option("versionAsOf", 2).load("/delta/transformed_table")


