#      This recipe will explore how to modify the schema of a table named SquirrelObservations in Azure Databricks.  
# Reminder - this code should be executed in individual cells of Azure Databricks Notebooks

## Step 1
# Run the following code to create and modify the schema of the SquirrelObservations table in Azure Databricks

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("SquirrelObservationsApp").getOrCreate()

# Script 1: Define the schema and create the `SquirrelObservations` table
schema_statement = """
CREATE TABLE IF NOT EXISTS SquirrelObservations (
    AreaName STRING,
    AreaID INT,
    ParkName STRING,
    ParkID INT,
     Date2 date,

    StartTime STRING,
    EndTime STRING,
    TotalTime INT,
    ParkConditions STRING,
    OtherAnimalSightings STRING,
    Litter STRING,
    TemperatureWeather STRING,
    NumberOfSquirrels INT,
    SquirrelSighters STRING,
    NumberOfSighters INT
)  PARTITIONED BY (Date DATE)

"""

# Execute the schema creation statement
spark.sql(schema_statement)

## Step 2

# populate the table you just created with 10 rows of data
# Script 2: Populate the `SquirrelObservations` table with 10 rows of sample data
insert_statement = """
INSERT INTO SquirrelObservations
VALUES
('Maple Grove', 101, 'Liberty Park', 201, '2024-02-16', '08:30', '09:45', 75, 'Sunny', 'Foxes', 'Minimal', '15°C Sunny', 4, 'Alice Brown', 2),
('Pine Hill', 102, 'Sunshine Park', 202, '2024-02-17', '09:00', '11:00', 120, 'Partly Cloudy', 'Rabbits', 'Moderate', '18°C Breezy', 3, 'Charlie Grey', 1),
('Oak Ridge', 103, 'Riverfront Park', 203, '2024-02-18', '10:00', '12:00', 120, 'Foggy', 'Deer', 'Scattered', '10°C Fog', 5, 'Diana Gold', 2),
('Cedar Woods', 104, 'Heritage Park', 204, '2024-02-19', '11:00', '13:00', 120, 'Misty', 'None', 'None', '12°C Mist', 2, 'Ethan Black', 1),
('Birch Meadow', 105, 'Prospect Park', 205, '2024-02-20', '12:00', '14:00', 120, 'Rainy', 'Birds', 'Heavy', '7°C Rain', 8, 'Fiona Cyan', 3),
('Elm Corner', 106, 'Greenwood Park', 206, '2024-02-21', '13:00', '15:00', 120, 'Clear', 'Coyotes', 'Low', '20°C Clear', 7, 'George Teal', 2),
('Spruce Nook', 107, 'Canyon Park', 207, '2024-02-22', '14:00', '16:00', 120, 'Windy', 'Raccoons', 'Minimal', '22°C Windy', 6, 'Hannah Silver', 1),
('Willow Vale', 108, 'Harbor Park', 208, '2024-02-23', '15:00', '17:00', 120, 'Cloudy', 'None', 'None', '16°C Overcast', 4, 'Ian Violet', 2),
('Larch Grove', 109, 'Pioneer Park', 209, '2024-02-24', '16:00', '18:00', 120, 'Hot', 'Squirrels', 'Moderate', '30°C Hot', 9, 'Jade Maroon', 3),
('Willow Bend', 110, 'Meadow Park', 210, '2024-02-25', '17:00', '19:00', 120, 'Cool', 'Foxes', 'Scattered', '14°C Cool Evening', 5, 'Kyle Indigo', 2)
"""

# Execute the insert statement
spark.sql(insert_statement)

## Step 3

# confirm the data is present
# Script to query data with a specific condition from the `SquirrelObservations` table
query_with_filters_statement = """
SELECT * FROM SquirrelObservations
WHERE NumberOfSquirrels > 5
"""

# Execute the query and show the results with the condition applied
filtered_results = spark.sql(query_with_filters_statement)
filtered_results.show()

## Step 4

# Identify the new columns you need to add to the SquirrelObservations table

# Initialize Spark session with Delta Lake's schema auto-merge feature enabled
spark = SparkSession.builder.appName("SquirrelObservationsApp") \
                    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
                    .getOrCreate()

# Read the existing Delta table into a DataFrame
df = spark.read.format("delta").table("SquirrelObservations")

# Add four new columns with default values
df = df.withColumn("NewColumn1", lit(None).cast("String")) \
       .withColumn("NewColumn2", lit(None).cast("Integer")) \
       .withColumn("NewColumn3", lit(None).cast("String")) \
       .withColumn("NewColumn4", lit(None).cast("String"))

# Overwrite the existing Delta table with the new DataFrame that includes the new columns
# Delta Lake's schema auto-merge feature will handle the schema update
df.write.format("delta").mode("overwrite").saveAsTable("SquirrelObservations")


## Step 5

# query the table to see the new changes
# Script to query data with a specific condition from the `SquirrelObservations` table
query_with_filters_statement = """
SELECT * FROM SquirrelObservations
WHERE NumberOfSquirrels > 5
"""

# Execute the query and show the results with the condition applied
filtered_results = spark.sql(query_with_filters_statement)
filtered_results.show()


## Step 6

# perform schema changes
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType

# Initialize Spark session with Delta Lake's schema auto-merge feature enabled
spark = SparkSession.builder.appName("ModifySchemaApp") \
                    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
                    .getOrCreate()

# Read the existing Delta table into a DataFrame
df = spark.read.format("delta").table("SquirrelObservations")

# Select all columns but cast the types of 3 columns to new types
new_schema_df = df.withColumn("AreaID", col("AreaID").cast(StringType())) \
                  .withColumn("ParkID", col("ParkID").cast(StringType())) \
                  .withColumn("NumberOfSquirrels", col("NumberOfSquirrels").cast(IntegerType()))

# Overwrite the existing Delta table with the new DataFrame that has a modified schema
new_schema_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("SquirrelObservations")

## Step 7

# replace the content of 3 specific records in the table
from pyspark.sql import SparkSession
from datetime import date

# Initialize Spark session
spark = SparkSession.builder.appName("UpdateRecordsApp").getOrCreate()

# Read the existing Delta table into a DataFrame
df = spark.read.format("delta").table("SquirrelObservations")

# Define the new records with updated content
new_records = spark.createDataFrame([
    ("Maple Grove", "101", "Liberty Park", "201", date(2024, 2, 16), "08:30", "09:45", 75, "Sunny", "Foxes", "Minimal", "15°C Sunny", 4, "Alice Brown", 2,0,0,0,0),
    ("Pine Hill", "102", "Sunshine Park", "202", date(2024, 2, 17), "09:00", "11:00", 120, "Partly Cloudy", "Rabbits", "Moderate", "18°C Breezy", 3, "Charlie Grey", 1,0,0,0,0),
    ("Oak Ridge", "103", "Riverfront Park", "203", date(2024, 2, 18), "10:00", "12:00", 120, "Foggy", "Deer", "Scattered", "10°C Fog", 5, "Diana Gold", 2,0,0,0,0)
], df.schema) # Ensure the new DataFrame conforms to the existing schema

# Filter out the old records that will be replaced
filtered_df = df.filter(~(df["AreaID"] == "101") & ~(df["AreaID"] == "102") & ~(df["AreaID"] == "103"))

# Union the old DataFrame without the replaced records with the new records
final_df = filtered_df.union(new_records)

# Overwrite the existing Delta table with the new DataFrame that has the updated records
final_df.write.format("delta").mode("overwrite").saveAsTable("SquirrelObservations")

## Step 8

# use the mergeSchema option for renaming two columns in the SquirrelObservations Delta table
from pyspark.sql import SparkSession

# Initialize Spark session with Delta Lake support
spark = SparkSession.builder.appName("RenameColumnsApp").getOrCreate()

# Read the existing Delta table into a DataFrame
df = spark.read.format("delta").table("SquirrelObservations")

# Rename two columns in the DataFrame
df_renamed = df.withColumnRenamed("AreaID", "AreaIDRenamed") \
               .withColumnRenamed("ParkID", "ParkIDRenamed")

# Use the `mergeSchema` option to merge the DataFrame schema with the Delta table schema
df_renamed.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("SquirrelObservations")
