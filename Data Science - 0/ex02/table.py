import psycopg2 as psy
import pandas as pd
import os
import sqlalchemy
from sqlalchemy import create_engine


def load(path: str) -> pd.DataFrame:
    try:
        if not isinstance(path, str):
            return None
        elif not os.path.exists(path):
            return None
        elif not path.lower().endswith('.csv'):
            return None
        # Load the data into a DataFrame
        df = pd.read_csv(path)
        print("Loading dataset of dimension", str(df.shape))
    except AssertionError as e:
        print(e)
    return df


def main():
    try:
        #interact with PostgreSQL databases.
        # connection = psy.connect(
        #     host="localhost",
        #     port="5432",
        #     database="piscineds",
        #     user="ndormoy",
        #     password="mysecretpassword"
        # )
        #The cursor object acts as a handle to the database
        #and allows you to execute SQL statements on it.
        # cursor = connection.cursor()
        df = load("../subject/customer/data_2022_oct.csv")
        table_name = "data_2022_oct"
        columns = ", ".join(df.columns)
        #create a new database engine object
        engine = create_engine('postgresql://ndormoy:mysecretpassword@localhost:5432/piscineds')
        #write the DataFrame df into the data_2022_oct... table
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        # cursor.close()
        # connection.close()
        
    except psy.Error as e:
        print("Error connecting to the PostgreSQL database:", e)


if __name__ == '__main__':
    main()
    
