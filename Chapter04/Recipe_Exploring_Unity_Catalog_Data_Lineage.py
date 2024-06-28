from pyspark.sql import SparkSession

# Step 1: Initialize Spark session
spark = SparkSession.builder.appName("SquirrelDataAnalysis").getOrCreate()

# Step 2: Read CSV file into a DataFrame with schema inference
squirrels_df = spark.read.csv("/FileStore/tables/squirrel_data.csv", header=True, inferSchema=True)
squirrels_df.show()

# Step 3: Create a table from the DataFrame in the specified catalog and schema
squirrels_df.write.mode("overwrite").saveAsTable("Cataloganimals.SchemaSquirrels.SquirrelsData")

# Step 4: Query the newly created table to confirm data load
result = spark.sql("SELECT * FROM Cataloganimals.SchemaSquirrels.SquirrelsData LIMIT 10")
result.show()

# Additional Steps for Creating and Verifying a View
# Create or replace a temporary view for easier querying
spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW squirrel_data_view AS 
SELECT * FROM Cataloganimals.SchemaSquirrels.SquirrelsData
""")

# Display the first few rows of the view to verify its creation
view_result = spark.sql("SELECT * FROM squirrel_data_view LIMIT 5")
view_result.show()
