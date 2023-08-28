import pandas as pd
import os

# Define the directory where your CSV files are located
csv_directory = os.getcwd()
print('csvdir :' + csv_directory)

# Get a list of all CSV files in that directory
csv_files = [f for f in os.listdir(csv_directory) if f.endswith('.csv')]

# Initialize a dictionary to hold DataFrames. The keys will be the CSV filenames.
dataframes = {}

# Specify the chunk size for reading in the CSV
chunk_size = 50000  # you can change this size depending on your available memory

for csv_file in csv_files:
    chunks = []
    full_path = os.path.join(csv_directory, csv_file)
    for chunk in pd.read_csv(full_path, chunksize=chunk_size, encoding='utf-8', low_memory=False):
    # your code for processing each chunk

        chunks.append(chunk)

    # Combine chunks back into single dataframe
    df = pd.concat(chunks, axis=0)
    
    # Store the DataFrame in the dictionary
    dataframes[csv_file] = df

    # Print first few rows to check
    print(f"First few rows of {csv_file}:")
    print(df.head())
    print("\n---\n")

# Now, 'dataframes' is a dictionary containing all your DataFrames
