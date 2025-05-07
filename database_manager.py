from sqlalchemy import create_engine, text
import pandas as pd


# connection parameters
host = "localhost"
port = "5432"
database = "postgres"
user = "postgres"
password = "mysecretpassword"


def get_engine():
    """This function returns engine with its connection parameters"""
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")


def test_database():
    """Testing function"""
    engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        ))
        tables = [row[0] for row in result]
        print("Tables in db: ", tables)


def create_tables():
    """Create tables in the PostgreSQL database"""
    engine = get_engine()

    commands = (
        """
        CREATE TABLE IF NOT EXISTS referees (
            referee_id SERIAL PRIMARY KEY,
            referee_name VARCHAR(255) NOT NULL,
            avg_yellow_cards DOUBLE PRECISION NOT NULL,
            avg_red_cards DOUBLE PRECISION NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS competitions (
            competition_id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            country VARCHAR(255) NOT NULL,
            type VARCHAR(10) CHECK (type in ('league', 'cup'))
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS competition_seasons (
            competition_season_id SERIAL PRIMARY KEY,
            season INTEGER NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            duration_days INTEGER NOT NULL,
            competition_id INTEGER NOT NULL REFERENCES competitions(competition_id)
                ON DELETE CASCADE ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS standings (
            standing_id SERIAL PRIMARY KEY,
            competition_season_id INTEGER UNIQUE NOT NULL REFERENCES competition_seasons(competition_season_id)
                ON DELETE CASCADE ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS teams (
            team_id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            standing_id INTEGER NOT NULL REFERENCES standings(standing_id)
                ON DELETE CASCADE ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS team_stats (
            team_stats_id INTEGER PRIMARY KEY,
            rank INTEGER NOT NULL,
            points INTEGER NOT NULL,
            wins INTEGER NOT NULL,
            draws INTEGER NOT NULL,
            losses INTEGER NOT NULL,
            goals_scored INTEGER NOT NULL,
            goals_conceded INTEGER NOT NULL,
            points_per_game FLOAT NOT NULL,
            goal_difference INTEGER NOT NULL,
            FOREIGN KEY (team_stats_id) REFERENCES teams(team_id)
                ON DELETE CASCADE ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS matches (
            match_id SERIAL PRIMARY KEY,
            match_date TIMESTAMP NOT NULL,
            home_team VARCHAR(255) NOT NULL,
            away_team VARCHAR(255) NOT NULL,
            competition_season_id INTEGER NOT NULL REFERENCES competition_seasons(competition_season_id)
                ON DELETE CASCADE ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS match_stats (
            match_id INTEGER PRIMARY KEY REFERENCES matches(match_id)
                ON DELETE CASCADE ON UPDATE CASCADE,
            goals_home INTEGER NOT NULL,
            goals_away INTEGER NOT NULL,
            home_outcome VARCHAR(10) CHECK (home_outcome IN ('win', 'draw', 'lose')),
            home_halftime_outcome VARCHAR(10) CHECK (home_halftime_outcome IN ('win', 'draw', 'lose')),
            reversed_result BOOLEAN NOT NULL,
            referee_id INTEGER REFERENCES referees(referee_id)
                ON DELETE SET NULL ON UPDATE CASCADE
        )
        """
    )


    try:
        with engine.connect() as conn:
            for command in commands:
                conn.execute(text(command))
            conn.commit()
        print("Tables have been created successfully")
    except Exception as e:
        print(f"Exception while creating tables: {e}")

def drop_table(table: str):
    """This function drops table from input"""
    engine = get_engine()

    try:
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE {table} CASCADE"))
            conn.commit()
        print(f"Table {table} have been dropped!")
    except Exception as e:
        print(f"Exception while dropping table: {e}")


def read_referee_data():
    """Reads referee data from db"""
    engine = get_engine()

    with engine.connect() as conn:
        df = pd.read_sql(
            "SELECT * FROM referees;", con=engine)

        return df
    
def read_competitions_data():
    """Reads competitions data from db"""
    engine = get_engine()

    with engine.connect() as conn:
        df = pd.read_sql(
            "SELECT * FROM competitions;", con=engine)

        return df

def read_competition_seasons_data():
    """Reads competition seasons data from db"""
    engine = get_engine()

    with engine.connect() as conn:
        df = pd.read_sql(
            "SELECT * FROM competition_seasons;", con=engine)

        return df
    
def read_standings():
    """Reads standings data from db"""
    engine = get_engine()

    with engine.connect() as conn:
        df = pd.read_sql(
            "SELECT * FROM standings", con=engine
        )

        return df

def read_teams():
    """Reads teams data from db"""
    engine = get_engine()

    with engine.connect() as conn:
        df = pd.read_sql(
            "SELECT * FROM teams", con=engine
        )

        return df

print(read_competitions_data())    
print(read_competition_seasons_data())
print(read_standings())
print(read_teams())