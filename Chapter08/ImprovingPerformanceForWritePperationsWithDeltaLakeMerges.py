# Reminder - this code should be executed in individual cells of Azure Databricks Notebooks

# Strategy 1: Data segmentation
# Create a new cell and execute DESCRIBE DETAIL command to identify partition columns such as date or region. Identifying these columns will allow you to improve data segmentation

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("TableDetailsApp").getOrCreate()

# Execute DESCRIBE DETAIL command on the SquirrelObservations table
tableDetail = spark.sql("DESCRIBE DETAIL SquirrelObservations")

# Select and show the partitionColumns from the table details
tableDetail.select("partitionColumns").show()


# Strategy 2: Analysis of partition data
# Assuming date and region are identified as partition columns from the DESCRIBE DETAIL command output. Read the data that needs to be partitioned

data_to_partition = spark.read.format("delta").table("SquirrelObservations")

# Partition the data by date and region columns and write to a new Delta table or overwrite the existing one
data_to_partition.write \
    .format("delta") \
    .partitionBy("date", "region") \
    .mode("overwrite") \
    .save("/path/to/save/partitioned_table")

# Strategy 3: File compaction

# Small files in a Delta table can hinder performance. The following code will help to identify and consolidate them.
# Assuming spark session is already initialized
spark.sql("""
    OPTIMIZE delta.`dbfs:/user/hive/warehouse/squirrelobservations`
    WHERE Date > '2023-02-20'
""")

# o	You can use the following code to tune the SquirrelObservations table
spark.conf.set("spark.Sql.shuffle.partitions", 200)

# Enable optimized writes: To enable optimized writes before merging to reduce small file creation, you can run the following code
spark.conf.set("spark.databricks.delta.properties.defaults.optimizedWrite", "true")

# Implement Low Shuffle Merge: If available, Low Shuffle Merge enhances merge performance significantly. Here's the sample code to check compatibility
spark.Sql("SELECT version()").show()