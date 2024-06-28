

# Initialize Spark session for Customer Data Aggregation
spark = SparkSession.builder.appName('CustomerDataAggregation').getOrCreate()

# Function to list mounted directories
def list_mounted_dirs():
    mounts = dbutils.fs.mounts()
    return mounts

# Function to list files in a specified mount point
def list_files_in_directory(mount_point):
    file_list = dbutils.fs.ls(mount_point)
    return file_list

# Function to read a CSV file and show its content
def read_and_show_csv(file_path):
    df = spark.read.format('csv').option('header', 'true').load(file_path)
    df.show()
    return df

# Define path to the CSV file
file_path = '/mnt/your-mount-name/path/to/your-file.csv'
df = read_and_show_csv(file_path)

# Setup for data aggregation
schema = 'customer_id INT, name STRING, age INT, email STRING, customer_status STRING'
df = spark.readStream.format('cloudFiles').option('cloudFiles.format', 'csv')\\
    .option('path', file_path).option('header', 'true').schema(schema).load()

# Create a table using DataFrame
df.writeStream.format('delta').option('checkpointLocation', '/mnt/data/checkpoints').table('customers')

# Load the table as DataFrame
customers_df = spark.table('customers')

# Perform aggregation operations
aggregated_df = customers_df.groupBy('customer_status').agg(
    count('*').alias('total_customers'),
    avg('age').alias('average_age')
)

# Show the aggregation results
aggregated_df.show()

# Find and show the oldest customer per status
oldest_customer_df = customers_df.groupBy('customer_status').agg(max('age').alias('max_age'))\\
    .join(customers_df, ['customer_status'], 'inner')\\
    .select('customer_status', 'name', 'age')\\
    .filter(col('age') == col('max_age'))
oldest_customer_df.show()

# Save the results for further analysis
aggregated_df.write.format('delta').mode('overwrite').save('/mnt/data/aggregated_customer_data')

# Initialize Spark session for Customer Data Transformation
spark = SparkSession.builder.appName('CustomerDataTransformation').getOrCreate()

# Auto Loader setup to ingest CSV data and define a table for SQL operations
customers_df = spark.readStream.format('cloudFiles').option('cloudFiles.format', 'csv')\\
    .option('path', file_path).option('header', 'true').load()
customers_df.writeStream.format('delta').option('checkpointLocation', '/mnt/checkpoints').table('customers')

# Load the table and apply transformations
customers_df = spark.table('customers')
customers_df = customers_df.withColumn('email', lower(col('email')))\\
    .withColumn('age_group', when(col('age') < 18, 'Under 18')\\
    .when((col('age') >= 18) & (col('age') <= 35), '18-35')\\
    .when((col('age') > 35) & (col('age') <= 65), '36-65')\\
    .otherwise('Above 65'))\\
    .withColumn('name', initcap(col('name')))
customers_df.show()

# Save the transformed data
customers_df.write.format('delta').mode('overwrite').save('/mnt/data/transformed_customers')

# Incremental loading setup with Auto Loader
customers_df = spark.readStream.format('cloudFiles').option('cloudFiles.format', 'csv')\\
    .option('cloudFiles.useIncrementalListing', 'true').option('path', '/mnt/data/csv_files/')\\
    .option('header', 'true').schema('customer_id INT, name STRING, age INT, email STRING, customer_status STRING')\\
    .load()
customers_df.writeStream.format('delta').option('checkpointLocation', '/mnt/checkpoints/customers').table('customers')

# Apply transformations and write output continuously
transformed_df = customers_df.withColumn('email', lower(col('email')))\\
    .withColumn('age_group', when(col('age') < 18, 'Under 18')\\
    .when((col('age') >= 18) & (col('age') <= 35), '18-35')\\
    .when((col('age') > 35) & (col('age') <= 65), '36-65')\\
    .otherwise('Above 65'))\\
    .withColumn('name', initcap(col('name')))
query = transformed_df.writeStream.format('delta').option('checkpointLocation', '/mnt/checkpoints/transformed_customers')\\
    .outputMode('append').start('/mnt/data/transformed_customers')
query.awaitTermination()
"""
