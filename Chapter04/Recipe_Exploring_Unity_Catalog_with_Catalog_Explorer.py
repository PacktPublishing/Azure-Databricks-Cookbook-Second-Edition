
# Recipe: Exploring Unity Catalog with Catalog Explorer

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("CatalogExplorer").getOrCreate()

# Read the CSV file into a DataFrame
squirrels_df = spark.read.csv("/FileStore/tables/stories.csv", header=True, inferSchema=True)
squirrels_df.show()

# Write the DataFrame to a table
squirrels_df.write.mode("overwrite").saveAsTable("Cataloganimals.SchemaSquirrels.SquirrelsData")

# Query the newly created table
spark.sql("SELECT * FROM Cataloganimals.SchemaSquirrels.SquirrelsData LIMIT 10").show()
