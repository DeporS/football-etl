from sqlalchemy import create_engine, text
import pandas as pd


# connection parameters
host = "localhost"
port = "5432"
database = "postgres"
user = "postgres"
password = "mysecretpassword"


def load_data_to_referees(data: pd.DataFrame, table_name: str = "referees"):
    """Loading referee data into table"""
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{database}")

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text("TRUNCATE TABLE referees"))  # clear table

        data.to_sql(table_name, con=engine, if_exists='append',
                    index=False, method='multi')  # insert data from DataFrame



