import csv
from io import StringIO
import pandas as pd
import os
import sqlalchemy as sqlalch
from dotenv import load_dotenv
import psycopg2 as psycopg2
import time

load_dotenv()


def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Function {func.__name__} \
            took {end_time - start_time} seconds to run.")
        return result
    return wrapper


def create_table(conn, table_name: str, column_names: list):
    try:
        # Create a cursor object
        cur = conn.cursor()
        # Create the customers table if it doesn't exist
        columns = ", ".join([f"{col} TEXT" for col in column_names])
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
        # Commit the changes and close the cursor
        conn.commit()
        cur.close()
    except Exception as e:
        print("Error creating table:", e)


@timing_decorator
def load_csv_with_copy(csv_file_paths, table_name):
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")
    port = os.getenv("POSTGRES_PORT")
    # Connect to the database
    conn = psycopg2.connect(
        host="localhost",
        database=db,
        user=user,
        password=password,
        port=port
    )
    # Create a cursor object
    cur = conn.cursor()
    # Get column names from the first CSV file
    with open(csv_file_paths[0], "r") as f:
        reader = csv.reader(f)
        column_names = next(reader)
    create_table(conn, table_name, column_names)

    # Use the COPY command to load the combined data into the table
    combined_data = ""
    for csv_file_path in csv_file_paths:
        with open(csv_file_path, "r") as f:
            next(f)  # skip header row
            data = f.read()
            combined_data += data

    with conn.cursor() as cur:
        with StringIO(combined_data) as f:
            cur.copy_expert(f"COPY {table_name} FROM STDIN CSV HEADER", file=f)
    # The function commits the changes to the database
    conn.commit()
    # Close the cursor and connection
    cur.close()
    conn.close()


def main():
    """Main function"""
    try:
        print(pd.__version__)
        path = "customer"
        if not os.path.exists(path):
            raise ValueError(
                "Customer directory does not exist in current directory")
        csv_file_paths = [os.path.join(
            path, filename) for filename in os.listdir(path)
                if filename.lower().startswith(
                    'data_20') and filename.lower().endswith('.csv')]
        load_csv_with_copy(csv_file_paths, "customers")
    except AssertionError as e:
        print("Error connecting to the PostgreSQL database:", e)
    except ValueError as e:
        print(e)
    except sqlalch.exc.SQLAlchemyError as e:
        print(e)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()
