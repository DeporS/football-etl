from sqlalchemy import create_engine, text
import pandas as pd
from load.config.db_config import get_engine


def load_data_to_competitions(data: pd.DataFrame, table_name: str = "competitions"):
    """Loading competitions data into a table"""
    engine = get_engine()

    # Competitions table needs name, country, type("cup", "league")
    df = data[["name", "country", "type"]]
    df["type"] = df["type"].str.lower()
    df = df.drop_duplicates()

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))  # clear table

        df.to_sql(table_name, con=engine, if_exists='append',
                    index=False, method='multi')  # insert data from DataFrame


# load_data_to_competitions(pd.read_csv("data/transformed/transformed_leagues.csv"))