# Reminder - this code should be executed in individual cells of Azure Databricks Notebooks

## Strategy 1: Auto compaction

# # define an upper limit to the file size after compaction.

spark.conf.set("spark.databricks.delta.autoCompact.maxFileSize", "134217728")

# set compaction trigger

spark.conf.set("spark.databricks.delta.autoCompact.minNumFiles", "50")

# turn auto-compaction on a particular table

%Sql
ALTER TABLE your_table_name SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'auto')

# check if auto compaction is enabled for your table

%Sql
SHOW TBLPROPERTIES delta.`dbfs:/FileStore/<your_table>/data/`

## Strategy 2: Optimized writes

# enable table-level optimizations

%Sql
ALTER TABLE your_table_name SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')

# apply table level optimizations on a session level
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", 'true')

## Strategy 3: Setting target file size

# manually define target file size
%Sql
ALTER TABLE your_table_name SET TBLPROPERTIES ('delta.targetFileSize' = '104857600') 
