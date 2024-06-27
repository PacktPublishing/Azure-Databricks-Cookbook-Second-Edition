
# Recipe: Managing Storage and Location

# Example command to create an external location
spark.sql("CREATE EXTERNAL LOCATION my_external_location URL 's3a://my-bucket/my-location' WITH (STORAGE CREDENTIAL my_credential)")
