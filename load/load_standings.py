from sqlalchemy import create_engine, text
import pandas as pd
from config.db_config import get_engine


def load_data_to_standings(data: pd.DataFrame, season: int, table_name: str = "standings") -> int:
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

    with engine.begin() as conn:
        # Check if record with competition_season_id already exists
        result = conn.execute(
            text(f"SELECT 1 FROM {table_name} WHERE competition_season_id = :competition_season_id"),
            {"competition_season_id": competition_season_id}
        ).fetchone()

        if result:
            # Remove if exists
            conn.execute(
                text(f"DELETE FROM {table_name} WHERE competition_season_id = :competition_season_id"),
                {"competition_season_id": competition_season_id}
            )

        # Add new record
        conn.execute(
            text(f"""
                INSERT INTO {table_name} (competition_season_id)
                VALUES (:competition_season_id)
            """),
            {"competition_season_id": competition_season_id}
        )

    return competition_season_id

# print(load_data_to_standings(pd.read_csv("data/transformed/transformed_standings.csv"), 2023))