import tempfile
import psycopg2
import os
from dotenv import load_dotenv
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import pandas.io.sql as sqlio


load_dotenv()


def read_sql_tmpfile(query, db_engine):
    with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
            query=query, head="HEADER"
        )
        conn = db_engine.raw_connection()
        cur = conn.cursor()
        cur.copy_expert(copy_sql, tmpfile)
        tmpfile.seek(0)

        dtype_mapping = {
            "event_time": str,
            "event_type": str,
            "product_id": str,
            "price": str,
            "user_id": str,
            "user_session": str
        }
        df = pd.read_csv(tmpfile, dtype=dtype_mapping)
        return df


def create_pie():

    db_params = {
        "user" : os.getenv("POSTGRES_USER"),
        "password" : os.getenv("POSTGRES_PASSWORD"),
        "db" : os.getenv("POSTGRES_DB"),
        "port" : os.getenv("POSTGRES_PORT"),
    }
    print
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@localhost:{db_params['port']}/{db_params['db']}"
    print(f"Database url = {db_url}")

    try:
        # Create an SQLAlchemy engine
        engine = create_engine(db_url)

        # Query to fetch data from the customers table
        query = "SELECT * FROM customers"
        print("OK")
        df = read_sql_tmpfile(query, engine)
        print(f"Fetching data : {df.shape}")

        # Process the DataFrame and create a dictionary to count event types
        event_type_counts = df['event_type'].value_counts().to_dict()

        # Create a pie chart using Seaborn
        plt.figure(figsize=(8, 6))
        sns.set_palette("pastel")
        plt.pie(x=event_type_counts.values(), labels=event_type_counts.keys(), autopct='%.0f%%')
        plt.title('Event Type Distribution (Excluding "event_type")')
        plt.show()

        return df
    except Exception as e:
        print("Error:", e)
        return None

def main():
    create_pie()


if __name__ == '__main__':
    main()