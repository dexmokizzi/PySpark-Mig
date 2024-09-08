# This script helps to migrate data from SSMS to Postgres using Spark SQL.

from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Transfer Orders Data from SQL Server to PostgreSQL") \
    .config("spark.driver.extraClassPath", "C:/path/to/mssql-jdbc-12.8.1.jre11.jar:C:/path/to/postgresql-42.2.5.jar") \
    .getOrCreate()

# Set log level to ERROR to minimize logs
spark.sparkContext.setLogLevel("ERROR")

# SQL Server connection details
sql_server_url = "jdbc:sqlserver://ADEMOLA\\SQLEXPRESS;databaseName=northwind;encrypt=true;trustServerCertificate=true;"
sql_server_properties = {
    "user": "ademola",
    "password": "savage",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Read from SQL Server
df_sql_server = spark.read.jdbc(url=sql_server_url, table="dbo.orders", properties=sql_server_properties)

# Register the SQL Server table as a temporary view in Spark SQL
df_sql_server.createOrReplaceTempView("orders_sql_server")

# Perform any transformations or queries using Spark SQL (optional)
df_orders = spark.sql("""
    SELECT id, employee_id, customer_id, order_date, shipped_date, shipper_id, ship_name, ship_address, ship_city, 
           ship_state_province, ship_zip_postal_code, ship_country_region, shipping_fee, taxes, payment_type, paid_date, 
           tax_status_id, status_id
    FROM orders_sql_server
""")

# PostgreSQL connection details
postgres_url = "jdbc:postgresql://localhost:5432/northwind"
postgres_properties = {
    "user": "postgres",
    "password": "savage",
    "driver": "org.postgresql.Driver"
}

# Write the result of the query to PostgreSQL
df_orders.write.jdbc(url=postgres_url, table="public.orders", mode="overwrite", properties=postgres_properties)

print("Data transferred successfully from SQL Server to PostgreSQL")

# Stop the Spark session
spark.stop()