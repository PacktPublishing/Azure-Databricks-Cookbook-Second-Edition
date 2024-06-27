
# Recipe: Audit

# Example command to enable diagnostic logs
spark.sql("SET DIAGNOSTIC LOGGING ON FOR CATALOG Cataloganimals TO 's3a://my-bucket/audit-logs/'")
