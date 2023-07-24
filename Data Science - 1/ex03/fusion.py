import csv
import psycopg2
import os
from dotenv import load_dotenv


load_dotenv()

# Show in the database with multiple entries
# SELECT *
# FROM customers
# WHERE user_id = '485174092'
#   AND event_type = 'cart'
#   AND price::numeric = 2.14;
def insert_data_from_csv():
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
    cursor = conn.cursor()

    try:
        # Open the CSV file and copy its contents to the customers table
        with open("items/data_2023_feb.csv", 'r') as csvfile:
            cursor.copy_from(csvfile, 'customers', sep=',')
        # Commit the changes
        conn.commit()

        print("Data inserted from CSV successfully.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        # Close the connection
        cursor.close()
        conn.close()


if __name__ == "__main__":
    insert_data_from_csv()