from sqlalchemy import create_engine, text
import pandas as pd
from load.config.db_config import get_engine


def load_data_to_matches(data: pd.DataFrame, season: int, table_name: str = "matches") -> int:
    """Loading matches data into a table"""
    engine = get_engine()

    # needs match_date, home_team, away_team; also league and country for later joins
    df = data[["home_team", "away_team", "country", "league"]]
    df["match_date"] = data["date_utc"]

    query = """
        SELECT s.competition_season_id
        FROM competition_seasons s
        JOIN competitions c ON s.competition_id = c.competition_id
        WHERE c.name = :name AND s.season = :season AND c.country = :country
        """

    df_competitions = pd.read_sql(text(query), engine, params={"name": df["league"][0], "season": season, "country": df["country"][0]})

    if df_competitions.empty:
        print("No matching competition_season_id found.")
        return

    competition_season_id = int(df_competitions["competition_season_id"].iloc[0])

    df["competition_season_id"] = competition_season_id

    df.drop(columns=["country", "league"], inplace=True)

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        # dont need to clear old data, because if standings table changes it already deletes this data on cascade

        df.to_sql(table_name, con=engine, if_exists='append',
                    index=False, method='multi')  # insert data from DataFrame

    return competition_season_id

# print(load_data_to_matches(pd.read_csv("data/transformed/transformed_fixtures.csv"), 2023))