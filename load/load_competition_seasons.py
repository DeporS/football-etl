from sqlalchemy import create_engine, text
import pandas as pd
from config.db_config import get_engine


def load_data_to_competition_seasons(data: pd.DataFrame, table_name: str = "competition_seasons"):
    """Loading competition seasons data into a table"""
    engine = get_engine()

    # Competition seasons table needs season, start_date, end_date, duration_days, competition_id (FK), name and country for mapping
    df_seasons = data[["season", "start_date", "end_date", "duration_days", "name", "country"]]

    # getting competition_id mapped with its names
    df_competitions = pd.read_sql("SELECT competition_id, name, country FROM competitions", engine)

    # Mapping competition_name from seasons to competitions
    df_merged = df_seasons.merge(df_competitions, how="left", on=["name", "country"]) # FK

    # Removing useless column
    df_merged.drop(columns=["name", "country"], inplace=True)


    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))  # clear table

        df_merged.to_sql(table_name, con=engine, if_exists='append',
                    index=False, method='multi')  # insert data from DataFrame


# load_data_to_competition_seasons(pd.read_csv("data/transformed/transformed_leagues.csv"))