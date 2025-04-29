from sqlalchemy import create_engine, text
import pandas as pd


# connection parameters
host = "localhost"
port = "5432"
database = "postgres"
user = "postgres"
password = "mysecretpassword"


def create_tables(engine):
    """Create tables in the PostgreSQL daatabase"""
    commands = (
        """
        CREATE TABLE IF NOT EXISTS referees (
            referee_id SERIAL PRIMARY KEY,
            referee_name VARCHAR(255) NOT NULL,
            avg_yellow_cards DOUBLE PRECISION NOT NULL,
            avg_red_cards DOUBLE PRECISION NOT NULL
        )
        """,
    )

    try:
        with engine.connect() as conn:
            for command in commands:
                conn.execute(text(command))
        print("Tables have been created successfully")
    except Exception as e:
        print(f"Exception while creating tables: {e}")


def test(engine):
    """Testing function"""
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        ))
        tables = [row[0] for row in result]
        print("Tables in db: ", tables)


def load_data_to_referees(data: pd.DataFrame, table_name: str = "referees"):
    """Loading referee data into table"""
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{database}")

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text("TRUNCATE TABLE referees"))  # clear table

        data.to_sql(table_name, con=engine, if_exists='append',
                    index=False, method='multi')  # insert data from DataFrame


def read_referee_data():
    """Reading referee data from db"""
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{database}")

    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM referees;", con=engine)

        return df


# engine for connecting to db
engine = create_engine(
    f"postgresql://{user}:{password}@{host}:{port}/{database}")

# create tables
# create_tables(engine)
test(engine)
