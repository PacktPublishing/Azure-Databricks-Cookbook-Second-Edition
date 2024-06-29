# Compacting data files 
# Reminder - this code should be executed in individual cells of Azure Databricks Notebooks

# Create a Delta table with a structure that matches your CSV files
%Sql 
CREATE TABLE IF NOT EXISTS transactions_delta ( column1 INT, column2 STRING, ... ) USING DELTA;

# import each file into the table
file_names = ['1.csv', '2.csv', '3.csv'] for file_name in file_names: file_path = '/FileStore/tables/' + file_name df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path) df.write.format("delta").mode("append").saveAsTable("transactions_delta")

# check if data is imported
%Sql 
SELECT * FROM transactions_delta LIMIT 10;

# Optimize the table for each file
%Sql 
OPTIMIZE transactions_delta WHERE origin_file = '1.csv'; OPTIMIZE transactions_delta WHERE origin_file = '2.csv'; OPTIMIZE transactions_delta WHERE origin_file = '3.csv';

