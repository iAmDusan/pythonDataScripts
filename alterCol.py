import pyodbc
import configparser
import re

def read_alter_statements(filename):
    with open(filename, 'r') as f:
        alter_statements = f.readlines()
    return alter_statements

def execute_alter_statements(connection, alter_statements, varchar_max_columns):
    cursor = connection.cursor()
    total_statements = len(alter_statements)
    success = True

    try:
        connection.autocommit = False

        for idx, statement in enumerate(alter_statements, start=1):
            try:
                column_name = re.search(r'ALTER COLUMN \[([^\]]+)\]', statement).group(1)

                # Skip if this column is not in the list of VARCHAR(MAX) columns
                if column_name not in varchar_max_columns:
                    print(f"Skipped ALTER statement {idx}/{total_statements}: Column '{column_name}' is not VARCHAR(MAX).")
                    continue

                cursor.execute(statement)
                print(f"Executed ALTER statement {idx}/{total_statements}: {statement.strip()}")

            except Exception as e:
                print(f"Error executing ALTER statement {idx}/{total_statements}: {e}")
                success = False
                break

        if success:
            connection.commit()
            print("All ALTER statements executed successfully.")
        else:
            connection.rollback()
            print("Transaction rolled back due to errors.")
    except Exception as ex:
        print(f"Transaction aborted: {ex}")
        connection.rollback()
    finally:
        connection.autocommit = True


def get_varchar_max_columns(cursor):
    cursor.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'Cases' AND DATA_TYPE = 'varchar' AND CHARACTER_MAXIMUM_LENGTH = -1
    """)
    varchar_max_columns = [row[0] for row in cursor.fetchall()]
    return varchar_max_columns


def connect_to_db():
    config = configparser.ConfigParser()
    config.read('config.ini')
    server = config['SQL_SERVER']['Server']
    database = config['SQL_SERVER']['Database']
    username = config['SQL_SERVER']['Username']
    password = config['SQL_SERVER']['Password']

    connection_string = f"DRIVER=SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}"
    
    try:
        connection = pyodbc.connect(connection_string)
        return connection
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def main():
    connection = connect_to_db()
    if connection is None:
        return

    alter_statements = read_alter_statements('alter_statements.txt')
    varchar_max_columns = get_varchar_max_columns(connection.cursor())

    # Filter only relevant ALTER statements that correspond to VARCHAR(MAX) columns
    relevant_alter_statements = [stmt for stmt in alter_statements if re.search(r'ALTER COLUMN \[([^\]]+)\]', stmt).group(1) in varchar_max_columns]

    if not relevant_alter_statements:
        print("No relevant ALTER statements to execute.")
        connection.close()
        return

    print("The following ALTER statements will be executed:")
    for stmt in relevant_alter_statements:
        print(stmt.strip())

    confirmation = input(f"Are you sure you want to execute {len(relevant_alter_statements)} ALTER statements? (yes/no): ")
    if confirmation.lower() != 'yes':
        print("Execution aborted.")
        connection.close()
        return

    execute_alter_statements(connection, relevant_alter_statements, varchar_max_columns)

    connection.close()




if __name__ == "__main__":
    main()
