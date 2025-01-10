import sqlite3
import os
import zstandard as zstd

def main():
    # Ask user for input file
    input_file = input("Enter the path of the file to parse: ").strip().strip('"')
    if not os.path.exists(input_file):
        print("File does not exist.")
        return

    # Ask user for delimiter
    delimiter = input("Enter the delimiter used in the file (e.g., ':', ',', '\t'): ")

    # Ask user for number of columns (delimiters + 1)
    while True:
        try:
            num_columns = int(input("Enter the number of columns (number of delimiters + 1): "))
            break
        except ValueError:
            print("Please enter a valid number.")

    # Ask user to name the columns
    columns = []
    for i in range(num_columns):
        column_name = input(f"Enter name for column {i + 1}: ").strip()
        columns.append(column_name)

    # Ask user for SQLite database directory
    db_dir = input("Enter the directory where the SQLite database should be saved (e.g., '/path/to/'): ").strip().strip('"')
    if db_dir and not os.path.exists(db_dir):
        print(f"The directory '{db_dir}' does not exist.")
        return

    # Ask user for SQLite database file name
    db_name = input("Enter the name for the SQLite database file (e.g., 'output.db'): ").strip().strip('"')
    if not db_name.endswith(".db"):
        db_name += ".db"
    db_path = os.path.join(db_dir, db_name) if db_dir else db_name

    # Create SQLite connection
    try:
        conn = sqlite3.connect(db_path)
    except sqlite3.OperationalError as e:
        print(f"Error creating database file: {e}")
        return

    cursor = conn.cursor()

    # Create table with user-defined columns
    table_name = input("Enter the name for the table: ").strip()
    create_table_query = f"CREATE TABLE {table_name} ({', '.join([f'{col} TEXT' for col in columns])});"
    cursor.execute(create_table_query)

    # Determine if the file is .zst compressed
    is_zst = input_file.endswith(".zst")

    # Read and parse the file, then insert into the database
    if is_zst:
        with open(input_file, 'rb') as file:
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(file) as reader:
                for line in reader:
                    line = line.decode('utf-8').strip()
                    values = line.split(delimiter)
                    if len(values) != num_columns:
                        print(f"Skipping line due to incorrect number of columns: {line.strip()}")
                        continue

                    placeholders = ", ".join(["?"] * num_columns)
                    insert_query = f"INSERT INTO {table_name} VALUES ({placeholders});"
                    cursor.execute(insert_query, values)
    else:
        with open(input_file, 'r') as file:
            for line in file:
                values = line.strip().split(delimiter)
                if len(values) != num_columns:
                    print(f"Skipping line due to incorrect number of columns: {line.strip()}")
                    continue

                placeholders = ", ".join(["?"] * num_columns)
                insert_query = f"INSERT INTO {table_name} VALUES ({placeholders});"
                cursor.execute(insert_query, values)

    # Commit changes and close connection
    conn.commit()
    conn.close()

    print(f"Data successfully parsed and saved to '{db_path}' in the table '{table_name}'.")

if __name__ == "__main__":
    main()
