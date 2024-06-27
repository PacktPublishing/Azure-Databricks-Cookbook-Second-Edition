
# Recipe: Managing Security in Unity Catalog

# Example SQL commands to manage security
spark.sql("GRANT USE CATALOG ON CATALOG Cataloganimals TO user")
spark.sql("GRANT USE SCHEMA ON SCHEMA SchemaSquirrels TO user")
spark.sql("GRANT SELECT ON TABLE Cataloganimals.SchemaSquirrels.SquirrelsData TO user")
