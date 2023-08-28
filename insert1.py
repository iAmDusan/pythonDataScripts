import pyodbc
import pandas as pd
import os
import configparser
import glob
import time
import numpy as np

# Read database configurations from config.ini
config = configparser.ConfigParser()
config.read('config.ini')

conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config['SQL_SERVER']['Server']};DATABASE={config['SQL_SERVER']['Database']};UID={config['SQL_SERVER']['Username']};PWD={config['SQL_SERVER']['Password']}"

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.fast_executemany = True  # Enabling fast_executemany
    print("Successfully connected to the SQL Server.")
except Exception as e:
    print(f"Failed to connect to SQL Server: {e}")
    exit()

csv_files = [f for f in glob.glob("*.csv")]
print("Available CSV files in the current directory:")
for idx, file in enumerate(csv_files):
    print(f"{idx + 1}. {file}")

csv_file = input("Enter the name or number of the CSV file you want to import: ")
if csv_file.isnumeric():
    csv_file = csv_files[int(csv_file) - 1]

print(f"You've selected {csv_file}. Is that correct? (y/n): ")
confirmation = input()
if confirmation.lower() != 'y':
    print("Aborted.")
    exit()

# Fetch table names in the SQL Server database
cursor.execute("SELECT table_name = t.name FROM sys.tables t INNER JOIN sys.schemas s ON t.schema_id = s.schema_id;")
tables = [row.table_name for row in cursor.fetchall()]

# Display table names to the user
print("Available tables in the database:")
for table in tables:
    print(table)

# User selects the SQL table
table_name = input("Enter the SQL table name where the data will be inserted: ")

try:
    # Read CSV in chunks
    chunk_iter = pd.read_csv(csv_file, chunksize=5000, low_memory=False) # Adjust chunksize based on available memory
    print("Reading the CSV in chunks...")
    
    for chunk in chunk_iter:
        chunk = chunk.replace({np.nan: "NULL"})
        total_rows = len(chunk)
        print(f"Total rows in this chunk: {total_rows}")

        rows = [tuple(x) for x in chunk.to_numpy()]

        placeholders = ",".join("?" * len(chunk.columns))
        columns = ",".join([f"[{col}]" for col in chunk.columns])
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        start_time = time.time()
        cursor.executemany(sql, rows)
        conn.commit()
        end_time = time.time()
        
        print(f"Inserted this chunk in {end_time - start_time:.2f} seconds.")

except Exception as e:
    print(f"An error occurred: {e}")
    conn.rollback()

finally:
    cursor.close()
    conn.close()
