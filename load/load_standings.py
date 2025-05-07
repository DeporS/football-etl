from sqlalchemy import create_engine, text
import pandas as pd
from config.db_config import get_engine


def load_data_to_standings(data: pd.DataFrame, season: int, table_name: str = "standings"):
    """Loading standings data into a table"""
    engine = get_engine()

    # needs league name, country - both for mapping, competition_season_id as FK
    df = data[["league", "country"]]

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

    upsert_sql = text("""
        INSERT INTO standings (competition_season_id)
        VALUES (:competition_season_id)
        ON CONFLICT (competition_season_id)
        DO UPDATE SET standing_id = EXCLUDED.standing_id
    """)

    with engine.begin() as conn:
        conn.execute(upsert_sql, {"competition_season_id": competition_season_id})

load_data_to_standings(pd.read_csv("data/transformed/transformed_standings.csv"), 2023)