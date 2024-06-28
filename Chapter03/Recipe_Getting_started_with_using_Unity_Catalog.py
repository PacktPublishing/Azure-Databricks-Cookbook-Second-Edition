%sql
-- Step 1: Create the Unity Catalog if it doesn't already exist
-- Replace <catalog-name> with your desired catalog name.
CREATE CATALOG IF NOT EXISTS <catalog-name>;

-- Step 2: Specify an alternative managed storage location for the catalog
-- Replace <catalog-name>, <external-location-name>, and <directory-name> with your specific details.
CREATE CATALOG IF NOT EXISTS <catalog-name>
  MANAGED LOCATION 'abfss:<external-location-name>/<directory-name>';

