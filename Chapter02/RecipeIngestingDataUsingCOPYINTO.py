#Review the sample Customers CSV files:
display(dbutils.fs.ls("/mnt/adls/Common/Customer/csvFiles/")
# Ingesting CSV files
# Create a Delta table for the CSV files
%sql
CREATE TABLE IF NOT EXISTS Customers_CSV;

# Use COPY INTO to import CSV files into the Delta table
%sql
COPY INTO Customers_CSV
  FROM '/mnt/adls/Common/Customer/csvFiles/'
  FILEFORMAT = CSV
  PATTERN = 'part-0000[6-9]-*.csv'
  FORMAT_OPTIONS ('header' = 'true')
  COPY_OPTIONS ('mergeSchema' = 'true');

# Ingesting Parquet files
# Create a Delta table for the Parquet files
%sql
CREATE TABLE IF NOT EXISTS Customers_Parquet;

# Use COPY INTO to import Parquet files into the Delta table
%sql
COPY INTO Customers_Parquet
  FROM '/mnt/adls/Common/Customer/parquetFiles/'
  FILEFORMAT = PARQUET
  COPY_OPTIONS ('mergeSchema' = 'true');
