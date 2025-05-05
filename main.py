from extract.extract import extract_csv_to_dataframe, load_raw_json
from transform.transform import transform_data
from load import load_data_to_referees, read_referee_data
from extract.fetch_leagues import fetch_leagues
from extract.fetch_fixtures import fetch_fixtures
from extract.fetch_standings import fetch_standings
from transform.transform_leagues import transform_leagues
from transform.transform_fixtures import transform_fixtures
from transform.transform_standings import transform_standings
from database_manager import create_tables
import os
import json


def referees_etl():
    """etl that reads data from csv provided, transforms it, 
    selecting only referees and their stats, 
    and loads it into PostgreSQL referees table"""
    extracted_data = extract_csv_to_dataframe('data/E0.csv')
    transformed_data = transform_data(extracted_data)
    load_data_to_referees(transformed_data)
    print(read_referee_data())


def api_sports_etl(league_id: int, season: int):

    # Check if files already exist
    if not os.path.exists("data/raw/leagues.json"):
        print(f"Leagues data not found. Fetching...")
        fetch_leagues()

    if not os.path.exists(f"data/raw/fixtures_{league_id}_{season}.json"):
        print(f"Fixtures for league {league_id} in season {season} not found. Fetching...")
        fetch_fixtures(league_id, season)

    if not os.path.exists(f"data/raw/standings_{league_id}_{season}.json"):
        print(f"Standings for league {league_id} in season {season} not found. Fetching...")
        fetch_standings(league_id, season)

    
    raw_leagues = load_raw_json("data/raw/leagues.json")
    raw_fixtures = load_raw_json(f"data/raw/fixtures_{league_id}_{season}.json")
    raw_standings = load_raw_json(f"data/raw/standings_{league_id}_{season}.json")

    print(transform_leagues(raw_leagues))
    print(transform_fixtures(raw_fixtures))
    print(transform_standings(raw_standings))
    
    
    


def main():
    # api_sports_etl(39, 2023)
    # referees_etl()
    # create_tables()
    return


if __name__ == "__main__":
    main()
