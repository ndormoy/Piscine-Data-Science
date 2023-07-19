import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()


def load(path: str) -> pd.DataFrame:
    """Load a csv file from path"""
    try:
        if not isinstance(path, str):
            return None
        elif not os.path.exists(path):
            return None
        elif not path.lower().endswith('.csv'):
            return None
        # Load the data into a DataFrame
        df = pd.read_csv(path)
    except AssertionError as e:
        print(e)
    return df


def create_datatable(path, filename) -> None:
    """Create a datatable in postgres"""
    try:
        file_name_without_extension = os.path.splitext(filename)[0]
        path += '/' + filename
        print(file_name_without_extension)
        df = load(path)
        table_name = file_name_without_extension
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        db = os.getenv("POSTGRES_DB")
        port = os.getenv("POSTGRES_PORT")
        # create a new database engine object
        engine = create_engine(
            # 'postgresql://ndormoy:mysecretpassword@localhost:5432/piscineds')
            f'postgresql://{user}:{password}@localhost:{port}/{db}')
        # write the DataFrame df into the data_2022_oct... table
        df.to_sql(table_name, engine, if_exists='replace', index=False)
    except AssertionError as e:
        print(e)


def main():
    """Main function"""
    try:
        path = "customer"
        if not os.path.exists(path):
            raise ValueError(
                "Customer directory does not exist in current directory")
        for filename in os.listdir(path):
            print(f"filename = {filename}")
            if filename.lower().endswith('.csv'):
                create_datatable(path, filename)
            else:
                print(f"{filename}\
                    must be a csv file to be converted into a datatable")
    except AssertionError as e:
        print("Error connecting to the PostgreSQL database:", e)
    except ValueError as e:
        print(e)


if __name__ == '__main__':
    main()
