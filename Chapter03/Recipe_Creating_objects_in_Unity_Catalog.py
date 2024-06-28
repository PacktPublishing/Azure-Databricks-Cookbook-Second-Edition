#In some cases, you may prefer to script out the creation of the schema. In these cases, you can
#use SQL commands to automate the process.

#Replace <catalog-name>, <schema-name>, <table-name>, <view-name>, <volume-name>, and column definitions with actual values.
#Managed tables are best for fully controlled environments, while external tables are suited for integration with external systems.
#Views facilitate data representation and security, enabling standardized access to aggregated data.
#Choose between managed and external volumes based on whether the data resides within the Unity Catalog environment or in external storage solutions.
#This consolidated SQL block enables you to effectively manage various data structures within Azure Databricks' Unity Catalog, ensuring robust data governance and accessibility.
#Use a CREATE statement to create the new schema. Use dot notation to specify the name of
#the catalog in which the schema should be created and the name of the new schema:

CREATE SCHEMA <catalog-name>.<schema-name>

#If you would like to change the default storage location for the catalog, you can use the optional

MANAGED LOCATION clause:
CREATE SCHEMA <catalog-name>.<schema-name>
 MANAGED LOCATION '<path-to-external-location>'



%sql
-- Step 1: Create a Managed Table
-- Managed tables are controlled entirely by Unity Catalog, including their data lifecycle and file layout.
CREATE TABLE <catalog-name>.<schema-name>.<table-name> (
    <column-1-name> <column-1-datatype>,
    <column-2-name> <column-2-datatype>,
    ...
);

-- Step 2: Create an External Table
-- External tables allow data to reside outside Unity Catalog's managed storage, useful for direct external access.
CREATE TABLE <catalog-name>.<schema-name>.<table-name> (
    <column-1-name> <column-1-datatype>,
    <column-2-name> <column-2-datatype>,
    ...
) LOCATION 'abfs-path-to-data-location';

-- Step 3: Create a View
-- Views are used for creating readable and secured data aggregations accessible within Databricks.
CREATE VIEW <catalog-name>.<schema-name>.<view-name> AS
SELECT <column-names>
FROM <table-name>
WHERE <conditions>;

-- Step 4: Creating Volumes
-- Volumes manage access to unstructured data stored within or outside of Unity Catalog.

-- Creating a managed volume (stored in Unity Catalog's managed storage)
CREATE VOLUME <catalog-name>.<schema-name>.<volume-name>;

-- Creating an external volume (links to data stored in external locations)
CREATE VOLUME <catalog-name>.<schema-name>.<volume-name>
 LOCATION '<abfs-path-to-data-location>';
