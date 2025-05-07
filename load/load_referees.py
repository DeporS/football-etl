from sqlalchemy import create_engine, text
import pandas as pd
from load.config.db_config import get_engine


def load_data_to_referees(data: pd.DataFrame, table_name: str = "referees"):
    """Loading referee data into a table"""
    engine = get_engine()

    df = data[["referee_name", "avg_yellow_cards", "avg_red_cards"]]

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))  # clear table

        df.to_sql(table_name, con=engine, if_exists='append',
                    index=False, method='multi')  # insert data from DataFrame



