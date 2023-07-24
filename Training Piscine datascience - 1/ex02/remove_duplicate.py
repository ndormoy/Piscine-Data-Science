import os
from dotenv import load_dotenv
import psycopg2 as psycopg2


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

    try:

        cur.execute("""
            CREATE TABLE temp_table AS
            SELECT DISTINCT *
            FROM customers;
        """)

        # Step 2: Delete all rows from the original table
        cur.execute("""
            DELETE FROM customers;
        """)

        # Step 3: Insert distinct rows from the
        # temporary table back into the original table
        cur.execute("""
            INSERT INTO customers
            SELECT *
            FROM temp_table;
        """)

        # Commit the changes
        conn.commit()

        print("Duplicate rows removed successfully.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        # Close the connection
        cur.close()
        conn.close()


def main():
    try:
        remove_duplicates_from_table("customers")
    except AssertionError as e:
        print("Error connecting to the PostgreSQL database:", e)
    except ValueError as e:
        print(e)
    except psycopg2.OperationalError as e:
        print("Error connecting to the PostgreSQL database:", e)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
