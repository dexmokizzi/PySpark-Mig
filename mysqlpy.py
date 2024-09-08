from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Order") \
    .getOrCreate()

# Set Spark logging level to ERROR to reduce logs
spark.sparkContext.setLogLevel("ERROR")

# PostgreSQL connection details
postgres_url = "jdbc:postgresql://localhost:5432/northwind"
postgres_properties = {
    "user": "postgres",
    "password": "savage",
    "driver": "org.postgresql.Driver"
}

# Read the PostgreSQL data into a DataFrame
df_postgres = spark.read.jdbc(url=postgres_url, table="public.orders", properties=postgres_properties)

# Create a temporary view in Spark SQL for the PostgreSQL table
df_postgres.createOrReplaceTempView("orders_postgres")

# Perform any SQL queries or transformations using Spark SQL (optional)
df_orders_transformed = spark.sql("""
    SELECT id, employee_id, customer_id, order_date, shipped_date, shipper_id, ship_name, 
           ship_address, ship_city, ship_state_province, ship_zip_postal_code, ship_country_region, 
           shipping_fee, taxes, payment_type, paid_date, tax_status_id, status_id
    FROM orders_postgres
""")

# MySQL connection details
mysql_url = "jdbc:mysql://localhost:3306/northwind"
mysql_properties = {
    "user": "root",
    "password": "savage",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Write the transformed DataFrame to MySQL
df_orders_transformed.write.jdbc(url=mysql_url, table="orders", mode="overwrite", properties=mysql_properties)

print("Data successfully migrated from PostgreSQL to MySQL.")

# Stop the Spark session
spark.stop()
