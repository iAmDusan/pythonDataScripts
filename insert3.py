import pyodbc
import configparser
import glob
import time
import os
import pandas as pd
import numpy as np
from tqdm import tqdm



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
# chunk_size for READ ONLY
chunk_size = 50000

# Specify the engine and usecols parameter
total_csv_rows = 0

# Start the timer for CSV row counting
csv_row_count_start_time = time.time()

with pd.read_csv(csv_file, engine='c', usecols=[0], chunksize=chunk_size) as chunk_iter:
    for chunk in chunk_iter:
        total_csv_rows += len(chunk)

# Stop the timer for CSV row counting
csv_row_count_end_time = time.time()

print(f"Chunk size: {chunk_size}")
print(f"Total rows in the entire CSV: {total_csv_rows}")
print(f"Time taken for CSV row counting: {csv_row_count_end_time - csv_row_count_start_time:.2f} seconds")



cursor.execute("SELECT table_name = t.name FROM sys.tables t INNER JOIN sys.schemas s ON t.schema_id = s.schema_id;")
tables = [row.table_name for row in cursor.fetchall()]

print("Available tables in the database:")
print('\n'.join(tables))

table_name = input("Enter the SQL table name where the data will be inserted: ")
primary_key_column = input("Enter the name of the primary key or unique column for debugging (case-sensitive): ")





try:
    chunk_size = 50000  # Adjust based on observation and database capacity
    chunk_iter = pd.read_csv(csv_file, chunksize=chunk_size, low_memory=False, encoding='utf-8')
    
    pbar = tqdm(total=total_csv_rows, dynamic_ncols=True, unit="row")
    
    for i, chunk in enumerate(chunk_iter):
        # Debugging section
        if primary_key_column in chunk.columns:
            print(f"Checking column {primary_key_column}")
            print(f"Number of NaN/NULL values: {chunk[primary_key_column].isna().sum()}")
            print(f"Data types: {chunk[primary_key_column].apply(type).value_counts()}")
            print(f"Maximum length of values: {chunk[primary_key_column].apply(lambda x: len(str(x))).max()}")
        else:
            print(f"Column {primary_key_column} not found in this chunk.")
        
        chunk = chunk.replace({np.nan: "NULL"})
        total_rows = len(chunk)

        rows = [tuple(x) for x in chunk.to_numpy()]

        placeholders = ",".join("?" * len(chunk.columns))
        columns = ",".join([f"[{col}]" for col in chunk.columns])
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        start_time = time.time()
        try:
            cursor.executemany(sql, rows)
            conn.commit()
            end_time = time.time()
            pbar.update(total_rows)
        except Exception as e:
            print(f"[ERROR] An error occurred while inserting a row: {e}")
            for row in rows:
                try:
                    cursor.execute(sql, row)
                    conn.commit()
                except:
                    print(f"Problematic row: {row}")
                    conn.rollback()
                    break

        print(f"[SUCCESS] Inserted this chunk in {end_time - start_time:.2f} seconds.\n")

    pbar.close()

except Exception as e:
    print(f"[ERROR] An error occurred: {e}")
    conn.rollback()

finally:
    cursor.close()
    conn.close()







