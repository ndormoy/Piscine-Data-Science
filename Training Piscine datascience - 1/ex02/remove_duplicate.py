import csv
from io import StringIO
import pandas as pd
import os
import sqlalchemy as sqlalch
from sqlalchemy import create_engine
from dotenv import load_dotenv
import psycopg2 as psycopg2
import time


load_dotenv()


def remove_duplicates_from_table(table_name):
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    print(f"Password = {password}")
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

    # Remove duplicate rows using CTE and DELETE command
    with conn.cursor() as cur:
        cur.execute(f"""
            DELETE FROM {table_name}
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM {table_name}
                GROUP BY column1, column2, ...
            )
        """)

    # Commit the changes and close the cursor and connection
    conn.commit()
    cur.close()
    conn.close()
    
def main():
    try:
        print(pd.__version__)
        path = "customer"
        if not os.path.exists(path):
            raise ValueError(
                "Customer directory does not exist in the current directory")
    except AssertionError as e:
        print("Error connecting to the PostgreSQL database:", e)
    except ValueError as e:
        print(e)
    except psycopg2.OperationalError as e:
        print("Error connecting to the PostgreSQL database:", e)
    except Exception as e:
        print(e)
    remove_duplicates_from_table("customers")

if __name__ == "__main__":
    main()