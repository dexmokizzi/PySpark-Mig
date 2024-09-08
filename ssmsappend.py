from pyspark.sql import SparkSession

# Initialize a Spark session with explicit path configuration for the driver
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Insert Orders Data") \
    .getOrCreate()

# Set Spark logging level to ERROR to reduce logs
spark.sparkContext.setLogLevel("ERROR")

# Define the file path and read the CSV file into a DataFrame
file_path = r'C:\Users\HP\Desktop\DESKTOP\big query\orders.csv'
df = spark.read.option("header", "true").csv(file_path)

# Register the DataFrame as a temporary SQL view
df.createOrReplaceTempView("orders_temp")

# Use Spark SQL to transform the data
transformed_df = spark.sql("""
    SELECT 
        CASE WHEN id IS NULL THEN 0 ELSE CAST(id AS INT) END AS id,
        CASE WHEN employee_id IS NULL THEN 0 ELSE CAST(employee_id AS INT) END AS employee_id,
        CASE WHEN customer_id IS NULL THEN 0 ELSE CAST(customer_id AS INT) END AS customer_id,
        CASE WHEN shipper_id IS NULL THEN 0 ELSE CAST(shipper_id AS INT) END AS shipper_id,
        CASE WHEN tax_status_id IS NULL THEN 0 ELSE CAST(tax_status_id AS INT) END AS tax_status_id,
        CASE WHEN status_id IS NULL THEN 0 ELSE CAST(status_id AS INT) END AS status_id,
        CAST(order_date AS STRING) AS order_date,
        CAST(shipped_date AS STRING) AS shipped_date,
        CAST(paid_date AS STRING) AS paid_date,
        ship_name,
        ship_address,
        ship_city,
        ship_state_province,
        ship_zip_postal_code,
        ship_country_region,
        shipping_fee,
        taxes,
        payment_type,
        notes,
        tax_rate
    FROM orders_temp
""")

# Define SQL Server connection properties
url = "jdbc:sqlserver://ADEMOLA\\SQLEXPRESS;databaseName=northwind;encrypt=true;trustServerCertificate=true;"
properties = {
    "user": "ademola",
    "password": "savage",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Read existing data from the SQL Server table
existing_df = spark.read.jdbc(url=url, table="dbo.orders3", properties=properties)

# Perform a left anti-join to find new rows that do not exist in the existing table
new_data_df = transformed_df.join(existing_df, on="id", how="left_anti")

# Append only the new rows to the SQL Server table
new_data_df.write.jdbc(url=url, table="dbo.orders3", mode="append", properties=properties)

print("Data appended successfully, skipping existing IDs.")

# Stop the Spark session
spark.stop()