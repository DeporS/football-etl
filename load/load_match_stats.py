from sqlalchemy import create_engine, text
import pandas as pd
from load.config.db_config import get_engine


def get_match_id(row, competition_season_id, conn):
    """Gets the id of a match from teams column based on a name provided"""
    result = conn.execute(
        text("SELECT match_id FROM matches WHERE home_team = :home_team AND away_team = :away_team AND competition_season_id = :competition_season_id"),
        {"home_team": row["home_team"], "away_team": row["away_team"], "competition_season_id": competition_season_id}
    ).fetchone()
    return result[0] if result else None

def get_referee_id(row, conn):
    """Gets referee id based on the name from row"""
    result = conn.execute(
        text("SELECT referee_id FROM referees WHERE referee_name = :referee_name"),
        {"referee_name": row["referee"]}
    ).fetchone()
    return result[0] if result else None


def load_data_to_match_stats(data: pd.DataFrame, competition_season_id: int, table_name: str = "match_stats"):
    """Loading match stats data into a table"""
    engine = get_engine()

    # Team Stats table requires goals_home, goals_away, home_halftime_outcome, reversed_result; home_team, away_team for match_id; referee for referee_id
    df = data[["goals_home", "goals_away", "home_halftime_outcome", "home_outcome", "reversed_result", "home_team", "away_team", "referee"]]

    with engine.begin() as conn:
        df["match_id"] = df.apply(lambda row: get_match_id(row, competition_season_id, conn), axis=1)
        df["referee_id"] = df.apply(lambda row: get_referee_id(row, conn), axis=1)
        df["referee_id"] = df["referee_id"].astype("Int64")

    df.drop(columns=["home_team", "away_team", "referee"], inplace=True)

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        # dont need to clear old data, because if matches table changes it already deletes this data on cascade

        df.to_sql(table_name, con=engine, if_exists='append',
                    index=False, method='multi')  # insert data from DataFrame


# load_data_to_match_stats(pd.read_csv("data/transformed/transformed_fixtures.csv"), 7267)