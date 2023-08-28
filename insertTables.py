import pandas as pd
import pyodbc
import os
import configparser

# Retrieve DB credentials from the config
config = configparser.ConfigParser()
config.read('config.ini')
DB_DRIVER = config['SQL_SERVER']['Driver']
DB_SERVER = config['SQL_SERVER']['Server']
DB_NAME = config['SQL_SERVER']['Database']
DB_USERNAME = config['SQL_SERVER']['Username']
DB_PASSWORD = config['SQL_SERVER']['Password']

def read_csv_file(filename, nrows=None):
    return pd.read_csv(filename, nrows=nrows)

def infer_data_types(filename):
    df_sample = pd.read_csv(filename, nrows=5000)  # Limit rows for type inference
    
    return {col: "VARCHAR(MAX)" for col in df_sample.columns}



def get_connection():
    print("Attempting to connect to the SQL Server...")
    try:
        conn = pyodbc.connect(
            f'DRIVER={DB_DRIVER};'
            f'SERVER={DB_SERVER};'
            f'DATABASE={DB_NAME};'
            f'UID={DB_USERNAME};'
            f'PWD={DB_PASSWORD};'
        )
        print("Connection to the SQL Server established successfully!")
        return conn
    except Exception as e:
        print(f"Failed to connect to SQL Server. Error: {e}")
        return None

def create_table(conn, table_name, col_types):
    print("Attempting to create the table...")
    cursor = conn.cursor()
    
    if table_name.lower() == 'contact':
        # Create "Id" column as primary key only if it doesn't exist in the CSV columns
        if 'Id' not in col_types:
            columns = "[Id] INT PRIMARY KEY IDENTITY(1, 1)"
        else:
            columns = ""
        
        # Add remaining columns as VARCHAR(MAX)
        for col, dtype in col_types.items():
            if columns:
                columns += f", [{col}] VARCHAR(MAX)"
            else:
                columns += f"[{col}] VARCHAR(MAX)"
                
    else:
        # Surrounding column names with square brackets to handle potential SQL keywords or special characters in column names.
        columns = ", ".join([f"[{col}] {dtype}" for col, dtype in col_types.items()])
    
    query = f"CREATE TABLE dbo.{table_name} ({columns});"  # Note the 'dbo.' prefix
    
    try:
        cursor.execute(query)
        conn.commit()
        print(f"Table {table_name} created successfully!")
    except Exception as e:
        print(f"Error while creating table. Error: {e}")


# Adjustments in main function
def main():
    print("Starting the main function...")
    
    for filename in os.listdir():
        if filename.endswith('.csv'):
            choice = input(f"Do you want to process the file '{filename}'? (yes/no) ").lower()
            if choice not in ['yes', 'y']:
                continue
            
            print(f"Reading a sample of CSV file: {filename}")  
            col_types = infer_data_types(filename) # Pass filename
            
            # Connect to SQL Server
            conn = get_connection()
            if not conn:
                print("Failed to get database connection. Skipping this file.")
                continue

            # Create table, using the cleaned filename without extension as table name
            table_name = filename.split('.')[0]  # Removing the file extension here

            create_table(conn, table_name, col_types)

            conn.close()

if __name__ == '__main__':
    print("Script started...")
    main()
    print("Script ended...")
