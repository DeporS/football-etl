from extract.extract import extract_csv_to_dataframe
from transform.transform import transform_data
from load import load_data_to_referees, read_referee_data
from extract.fetch_leagues import fetch_leagues
from extract.fetch_fixtures import fetch_fixtures
from extract.fetch_standings import fetch_standings


def referees_etl():
    """etl that reads data from csv provided, transforms it, 
    selecting only referees and their stats, 
    and loads it into PostgreSQL referees table"""
    extracted_data = extract_csv_to_dataframe('data/E0.csv')
    transformed_data = transform_data(extracted_data)
    load_data_to_referees(transformed_data)
    print(read_referee_data())


def api_sports_etl(league_id: int, season: int):
    fetch_leagues()
    fetch_fixtures(league_id, season)
    fetch_standings(league_id, season)


def main():

    return


if __name__ == "__main__":
    main()
