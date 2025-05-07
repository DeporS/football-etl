from sqlalchemy import create_engine, text
import pandas as pd
from config.db_config import get_engine


def load_data_to_teams(data: pd.DataFrame, standing_id: int, table_name: str = "teams"):
    """Loading teams data into a table"""
    engine = get_engine()

    # Teams table name
    df = pd.DataFrame()
    df["name"] = data["team"]
    df["standing_id"] = standing_id

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        # dont need to clear old data, because if standings table changes it already deletes this data on cascade

        df.to_sql(table_name, con=engine, if_exists='append',
                    index=False, method='multi')  # insert data from DataFrame


# load_data_to_teams(pd.read_csv("data/transformed/transformed_standings.csv"), 14)