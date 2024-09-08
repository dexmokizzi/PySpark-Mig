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

# Write the transformed DataFrame to the SQL Server table using 'overwrite' mode
transformed_df.write.jdbc(url=url, table="dbo.orders3", mode="overwrite", properties=properties)

print("Data inserted successfully with overwrite mode.")

# Stop the Spark session
spark.stop()


# COMMAND TO RUN THIS: cd C:\Users\HP\Desktop\DAYTHREE\sparkScripts
# spark-submit --driver-class-path "file:///C:/Users/HP/jdbc/mssql-jdbc-12.8.1.jre11.jar" --jars "file:///C:/Users/HP/jdbc/mssql-jdbc-12.8.1.jre11.jar" sparksql.py
# spark-submit sparksql.py