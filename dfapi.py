from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

# Initialize a Spark session with explicit path configuration for the driver
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Insert Orders Data") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define the file path and read the CSV file into a DataFrame
file_path = r'C:\Users\HP\Desktop\DESKTOP\big query\orders.csv'
df = spark.read.option("header", "true").csv(file_path)

# Transform the DataFrame to ensure empty strings are replaced with None (NULL in SQL)
transformed_df = df \
    .withColumn("id", when(col("id").isNull(), lit(0)).otherwise(col("id").cast("int"))) \
    .withColumn("employee_id", when(col("employee_id").isNull(), lit(0)).otherwise(col("employee_id").cast("int"))) \
    .withColumn("customer_id", when(col("customer_id").isNull(), lit(0)).otherwise(col("customer_id").cast("int"))) \
    .withColumn("shipper_id", when(col("shipper_id").isNull(), lit(0)).otherwise(col("shipper_id").cast("int"))) \
    .withColumn("tax_status_id", when(col("tax_status_id").isNull(), lit(0)).otherwise(col("tax_status_id").cast("int"))) \
    .withColumn("status_id", when(col("status_id").isNull(), lit(0)).otherwise(col("status_id").cast("int"))) \
    .withColumn("order_date", when((col("order_date") == '') | (col("order_date").isNull()), lit(None)).otherwise(col("order_date").cast("string"))) \
    .withColumn("shipped_date", when((col("shipped_date") == '') | (col("shipped_date").isNull()), lit(None)).otherwise(col("shipped_date").cast("string"))) \
    .withColumn("paid_date", when((col("paid_date") == '') | (col("paid_date").isNull()), lit(None)).otherwise(col("paid_date").cast("string")))

# Define SQL Server connection properties
url = "jdbc:sqlserver://ADEMOLA\\SQLEXPRESS;databaseName=northwind;encrypt=true;trustServerCertificate=true;"
properties = {
    "user": "ademola",
    "password": "savage",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Write the transformed DataFrame to the SQL Server table using 'overwrite' mode
transformed_df.write.jdbc(url=url, table="dbo.orders6", mode="overwrite", properties=properties)

print("Data6 inserted successfully with NULLs where applicable.")

# Stop the Spark session
spark.stop()

# COMMAND TO RUN THIS:
# cd C:\Users\HP\Desktop\Workspace\PySpark-Mig>
# spark-submit dfapi.py

