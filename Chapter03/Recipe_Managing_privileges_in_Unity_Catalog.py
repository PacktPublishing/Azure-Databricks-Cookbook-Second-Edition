#In Unity Catalog, privileges are managed through SQL commands. To give a user or group privileges
#on an object, use the GRANT statement:

GRANT <privilege-name> ON <object-name> TO <user-or-group>

#For example, if you have a table named Customers in the default schema of the sales catalog
#and you want to grant read privileges on the table to the sales_analysts group, you can use the
#following command:

  GRANT SELECT ON sales.default.Customers TO sales_analysts

