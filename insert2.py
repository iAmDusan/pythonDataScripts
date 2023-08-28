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

csv_num = int(input("Enter the number of the CSV file you want to import: "))
csv_file = csv_files[csv_num - 1]

print(f"You've selected {csv_file}. Is that correct? (y/n): ")
confirmation = input()
if confirmation.lower() != 'y':
    print("Aborted.")
    exit()

# Calculate the total number of rows in the CSV file using pandas
chunk_size = 10000  # Adjust based on observation and database capacity
total_csv_rows = 0  # Initialize the total rows counter

with pd.read_csv(csv_file, chunksize=chunk_size, low_memory=False, encoding='utf-8') as chunk_iter:
    for chunk in chunk_iter:
        total_csv_rows += len(chunk)

print(f"Total rows in the entire CSV: {total_csv_rows}")

cursor.execute("SELECT table_name = t.name FROM sys.tables t INNER JOIN sys.schemas s ON t.schema_id = s.schema_id;")
tables = [row.table_name for row in cursor.fetchall()]

print("Available tables in the database:")
print('\n'.join(tables))

table_name = input("Enter the SQL table name where the data will be inserted: ")

try:
    chunk_size = 10000  # Adjust based on observation and database capacity
    chunk_iter = pd.read_csv(csv_file, chunksize=chunk_size, low_memory=False, encoding='utf-8')
    print("\n[INFO] Reading the CSV in chunks...\n")
    
    for i, chunk in enumerate(chunk_iter):
        chunk = chunk.replace({np.nan: "NULL"})
        total_rows = len(chunk)
        print(f"[PROGRESS] Inserting chunk with {total_rows} rows. Total rows inserted: {i * chunk_size + total_rows}/{total_csv_rows}...\n")

        rows = [tuple(x) for x in chunk.to_numpy()]

        placeholders = ",".join("?" * len(chunk.columns))
        columns = ",".join([f"[{col}]" for col in chunk.columns])
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        start_time = time.time()
        cursor.executemany(sql, rows)
        conn.commit()
        end_time = time.time()
        
        print(f"[SUCCESS] Inserted this chunk in {end_time - start_time:.2f} seconds.\n")

except Exception as e:
    print(f"[ERROR] An error occurred: {e}")
    conn.rollback()

finally:
    cursor.close()
    conn.close()
