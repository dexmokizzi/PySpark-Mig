import pandas as pd
import pyodbc

# Define the file path and read the CSV file
file_path = r'C:\Users\HP\Desktop\DESKTOP\big query\orders.csv'
df = pd.read_csv(file_path)

# Fill missing values for integer columns with 0 or another suitable default
df['id'] = df['id'].fillna(0).astype(int)
df['employee_id'] = df['employee_id'].fillna(0).astype(int)
df['customer_id'] = df['customer_id'].fillna(0).astype(int)
df['shipper_id'] = df['shipper_id'].fillna(0).astype(int)
df['tax_status_id'] = df['tax_status_id'].fillna(0).astype(int)
df['status_id'] = df['status_id'].fillna(0).astype(int)

# Ensure date columns are treated as strings
date_columns = ['order_date', 'shipped_date', 'paid_date']
for col in date_columns:
    df[col] = df[col].astype(str)  # Convert date columns to string (varchar) format

# Define the database connection parameters
server = 'ADEMOLA\SQLEXPRESS'
database = 'northwind'
username = 'ademola'
password = 'savage'

# Create a connection to the SQL Server database
connection_string = (
    f'DRIVER={{SQL Server}};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password};'
)

# Establish a connection
connection = pyodbc.connect(connection_string)
cursor = connection.cursor()

# Define the SQL table and columns to insert data
table_name = 'dbo.orders'  # Include schema name

# Delete all rows from the table before inserting new data
try:
    cursor.execute(f"DELETE FROM {table_name}")
    print("All rows deleted successfully.")
except Exception as e:
    print(f"Error deleting rows: {e}")

# Insert data into SQL Server
for index, row in df.iterrows():
    # Prepare the values for insertion with correct types
    values = []
    for value in row.values:
        if pd.isnull(value) or value == 'nan':  # Check if the value is null or 'nan'
            values.append('NULL')
        else:
            # Properly escape single quotes
            escaped_value = str(value).replace("'", "''")
            values.append(f"'{escaped_value}'")
    
    # Prepare the SQL statement
    sql = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(values)})"
    try:
        cursor.execute(sql)
    except Exception as e:
        print(f"Error inserting row {index}: {e}")

# Commit the transaction
connection.commit()

# Close the connection
cursor.close()
connection.close()

print("Data inserted successfully.")