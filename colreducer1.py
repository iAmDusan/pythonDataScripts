import pandas as pd
from collections import defaultdict
import os

def analyze_csv(filename, primary_column_name, chunksize=50000):
    # Initialize a dictionary to store the max length for each column
    column_max_lengths = defaultdict(int)

    # Read CSV in chunks
    chunk_iter = pd.read_csv(filename, chunksize=chunksize, low_memory=False)
    for chunk in chunk_iter:
        for column in chunk.columns:
            # Skip NaN values and find the max length for this chunk
            max_length = chunk[column].dropna().astype(str).apply(len).max()

            # Update global max length
            if max_length > column_max_lengths[column]:
                column_max_lengths[column] = max_length

    # Sort columns by length in descending order
    sorted_columns = sorted(column_max_lengths.items(), key=lambda x: x[1], reverse=True)

    # Generate CREATE TABLE statements
    create_statements = []
    for column, max_length in sorted_columns:
        # Adding a buffer for future data
        suggested_length = int(max_length + 10)

        # Handle column length conditions
        if column == primary_column_name:
            create_statements.insert(0, f"[{column}] VARCHAR(255) PRIMARY KEY")
        elif max_length == 18:
            create_statements.append(f"[{column}] VARCHAR({max_length})")
        elif suggested_length > 6000:
            create_statements.append(f"[{column}] VARCHAR(MAX)")
        else:
            create_statements.append(f"[{column}] VARCHAR({suggested_length})")

    # Concatenate the CREATE TABLE statements
    create_table_statement = "CREATE TABLE Cases (\n" + ",\n".join(create_statements) + "\n);"

    return create_table_statement

if __name__ == "__main__":
    filename = "case.csv"
    primary_column_name = input("Enter the name of the primary column (e.g., 'Case ID'): ")
    create_table_statement = analyze_csv(filename, primary_column_name)

    # Print to console
    print("\nGenerated CREATE TABLE statement:")
    print(create_table_statement)

    # Write to text file
    base_filename = os.path.splitext(os.path.basename(__file__))[0] + "_output"
    output_filename = base_filename + ".txt"
    counter = 1

    # Find a unique filename
    while os.path.exists(output_filename):
        output_filename = f"{base_filename}_{counter}.txt"
        counter += 1

    with open(output_filename, "w") as output_file:
        output_file.write(create_table_statement)

    print(f"\nOutput written to {output_filename}")
