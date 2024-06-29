# Reminder - this code should be executed in individual cells of Azure Databricks Notebooks

# reate new cell and run ANALYZE TABLE command on StatsExample1
%Sql
-- Comprehensive statistics collection for StatsExample1 
ANALYZE TABLE StatsExample1 COMPUTE STATISTICS FOR ALL COLUMNS;


# Create a second cell and modify the delta.checkpoint.writeStatsAsStruct property for StatsExample1 table
%Sql
-- Enabling efficient data retrieval for StatsExample1 ALTER TABLE StatsExample1 SET TBLPROPERTIES ('delta.checkpoint.writeStatsAsStruct' = 'true');

# Create new cell and tailor data skipping for the most frequently used columns for StatsExample1 by adjusting delta.dataSkippingNumIndexedCols property.
%Sql
-- Tailoring data skipping to the most queried columns in StatsExample1 ALTER TABLE StatsExample1 SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '20');


