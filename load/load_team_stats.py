from sqlalchemy import create_engine, text
import pandas as pd
from load.config.db_config import get_engine


def get_team_id(row, standing_id, conn):
    """Gets the id of a team from teams column based on a name provided"""
    result = conn.execute(
        text("SELECT team_id FROM teams WHERE name = :name AND standing_id = :standing_id"),
        {"name": row["team"], "standing_id": standing_id}
    ).fetchone()
    return result[0] if result else None


def load_data_to_team_stats(data: pd.DataFrame, standing_id: int, table_name: str = "team_stats"):
    """Loading team stats data into a table"""
    engine = get_engine()

    # Team Stats table requires rank, points, wins, draws, losses, goals_scored, goals_conceded, points_per_game, goal_difference; team for selecting id
    df = data[["rank", "points", "wins", "draws", "losses", "goals_scored", "goals_conceded", "points_per_game", "goal_difference", "team"]]

    with engine.begin() as conn:
        df["team_stats_id"] = df.apply(lambda row: get_team_id(row, standing_id, conn), axis=1)
    
    df.drop(columns=["team"], inplace=True)

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        # dont need to clear old data, because if standings table changes it already deletes this data on cascade

        df.to_sql(table_name, con=engine, if_exists='append',
                    index=False, method='multi')  # insert data from DataFrame


# load_data_to_team_stats(pd.read_csv("data/transformed/transformed_standings.csv"), 14)