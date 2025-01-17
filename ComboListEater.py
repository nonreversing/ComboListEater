import sqlite3
import os
import zstandard as zstd
import chardet  # USE pip install chardet IF NOT INSTALLED

def detect_encoding(file_path):
    """Detect encoding w/fallback."""
    with open(file_path, 'rb') as file:
        raw_data = file.read(10000)  # Read a portion of the file for detection
        result = chardet.detect(raw_data)
        encoding = result.get('encoding', 'utf-8')  # Default to UTF-8 if detection fails
        confidence = result.get('confidence', 0)
        print(f"Detected encoding: {encoding} (Confidence: {confidence})")
        if confidence < 0.5:  # If confidence is low, fallback to more flexible encode.
            print("Low confidence in detected encoding. Falling back to 'latin1'.")
            encoding = 'latin1'
        return encoding


def main():
    # Ask for input file
    input_file = input("Enter the path of the file to parse: ").strip().strip('"')
    if not os.path.exists(input_file):
        print("File does not exist.")
        return

    # Detect encoding
    file_encoding = detect_encoding(input_file)
    print(f"Detected file encoding: {file_encoding}")

    # Ask for delimiter
    delimiter = input("Enter the delimiter used in the file (e.g., ':', ',', '\\t'): ")

    # Ask for number of columns
    while True:
        try:
            num_columns = int(input("Enter the number of columns (number of delimiters + 1): "))
            break
        except ValueError:
            print("Please enter a valid number.")

    # Ask to name the columns
    columns = []
    for i in range(num_columns):
        column_name = input(f"Enter name for column {i + 1}: ").strip()
        columns.append(column_name)

    # Ask for SQLite database directory
    db_dir = input("Enter the directory where the SQLite database should be saved (e.g., '/path/to/'): ").strip().strip('"')
    if db_dir and not os.path.exists(db_dir):
        print(f"The directory '{db_dir}' does not exist.")
        return

    # Ask for SQLite database file name
    db_name = input("Enter the name for the SQLite database file (e.g., 'output.db'): ").strip().strip('"')
    if not db_name.endswith(".db"):
        db_name += ".db"
    db_path = os.path.join(db_dir, db_name) if db_dir else db_name

    # Check if file already exists and prompt for renaming
    if os.path.exists(db_path):
        print(f"The file '{db_path}' already exists.")
        while True:
            action = input("Do you want to overwrite it (O), rename it (R), or cancel (C)? ").strip().lower()
            if action == 'o':
                os.remove(db_path)  # Delete the existing database file
                print(f"File '{db_path}' has been overwritten.")
                break
            elif action == 'r':
                db_name = input("Enter a new name for the database file: ").strip().strip('"')
                if not db_name.endswith(".db"):
                    db_name += ".db"
                db_path = os.path.join(db_dir, db_name) if db_dir else db_name
                if not os.path.exists(db_path):
                    break
                else:
                    print(f"The file '{db_path}' also exists. Try another name.")
            elif action == 'c':
                print("Operation canceled.")
                return
            else:
                print("Invalid input. Please enter 'O', 'R', or 'C'.")

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

    # Handle existing tables
    try:
        cursor.execute(create_table_query)
    except sqlite3.OperationalError as e:
        if "already exists" in str(e):
            print(f"Table '{table_name}' already exists.")
            while True:
                action = input("Do you want to overwrite it (O), rename it (R), or cancel (C)? ").strip().lower()
                if action == 'o':
                    cursor.execute(f"DROP TABLE {table_name};")  # Drop the existing table
                    conn.commit()
                    cursor.execute(create_table_query)
                    print(f"Table '{table_name}' has been overwritten.")
                    break
                elif action == 'r':
                    table_name = input("Enter a new name for the table: ").strip()
                    create_table_query = f"CREATE TABLE {table_name} ({', '.join([f'{col} TEXT' for col in columns])});"
                    cursor.execute(create_table_query)
                    print(f"Table '{table_name}' has been created.")
                    break
                elif action == 'c':
                    print("Operation canceled.")
                    conn.close()
                    return
                else:
                    print("Invalid input. Please enter 'O', 'R', or 'C'.")
        else:
            print(f"Error creating table: {e}")
            conn.close()
            return

    # Determine if the file is .zst
    is_zst = input_file.endswith(".zst")

    # Read and parse the file, then insert into the database
    try:
        if is_zst:
            with open(input_file, 'rb') as file:
                dctx = zstd.ZstdDecompressor()
                with dctx.stream_reader(file) as reader:
                    for line in reader:
                        try:
                            line = line.decode(file_encoding).strip()
                            values = line.split(delimiter)
                            if len(values) != num_columns:
                                print(f"Skipping line due to incorrect number of columns: {line}")
                                continue

                            placeholders = ", ".join(["?"] * num_columns)
                            insert_query = f"INSERT INTO {table_name} VALUES ({placeholders});"
                            cursor.execute(insert_query, values)
                        except UnicodeDecodeError as e:
                            print(f"Skipping a line due to encoding error: {e}")
        else:
            with open(input_file, 'r', encoding=file_encoding, errors='replace') as file:
                for line in file:
                    values = line.strip().split(delimiter)
                    if len(values) != num_columns:
                        print(f"Skipping line due to incorrect number of columns: {line}")
                        continue

                    placeholders = ", ".join(["?"] * num_columns)
                    insert_query = f"INSERT INTO {table_name} VALUES ({placeholders});"
                    cursor.execute(insert_query, values)
    except Exception as e:
        print(f"Error during file processing: {e}")
        conn.close()
        return

    # Commit and close connection
    conn.commit()
    conn.close()

    print(f"Data successfully parsed and saved to '{db_path}' in the table '{table_name}'.")

if __name__ == "__main__":
    main()
