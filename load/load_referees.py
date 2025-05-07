from sqlalchemy import create_engine, text
import pandas as pd
from load.config.db_config import get_engine


def load_data_to_referees(data: pd.DataFrame, table_name: str = "referees"):
    """Loading referee data into a table"""
    engine = get_engine()

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text("TRUNCATE TABLE referees"))  # clear table

        data.to_sql(table_name, con=engine, if_exists='append',
                    index=False, method='multi')  # insert data from DataFrame



