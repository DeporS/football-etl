from extract.extract import extract_csv_to_dataframe, load_raw_json
from transform.transform import transform_referees_data
from load.load_referees import load_data_to_referees
from extract.fetch_leagues import fetch_leagues
from extract.fetch_fixtures import fetch_fixtures
from extract.fetch_standings import fetch_standings
from transform.transform_leagues import transform_leagues
from transform.transform_fixtures import transform_fixtures
from transform.transform_standings import transform_standings
from database_manager import create_tables, read_all_tables
from load.loader import load_data
import os
import json
import pandas as pd


def referees_etl():
    """etl that reads data from csv provided, transforms it, 
    selecting only referees and their stats, 
    and loads it into PostgreSQL referees table"""
    ## Extract
    extracted_data = extract_csv_to_dataframe('data/E0.csv')
    ## Transform
    transform_referees_data(extracted_data).to_csv("data/transformed/transformed_referees.csv")
    ## Load
    load_data_to_referees(pd.read_csv("data/transformed/transformed_referees.csv"))
    


def api_sports_etl(league_id: int, season: int):


    ## Extract
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

    ## Transform
    raw_leagues = load_raw_json("data/raw/leagues.json")
    raw_fixtures = load_raw_json(f"data/raw/fixtures_{league_id}_{season}.json")
    raw_standings = load_raw_json(f"data/raw/standings_{league_id}_{season}.json")

    transform_leagues(raw_leagues).to_csv("data/transformed/transformed_leagues.csv")
    transform_fixtures(raw_fixtures).to_csv(f"data/transformed/transformed_fixtures_{league_id}_{season}.csv")
    transform_standings(raw_standings).to_csv(f"data/transformed/transformed_standings_{league_id}_{season}.csv")
    
    ## Load
    load_data(season, league_id)


def print_database():
    read_all_tables()


def main():
    referees_etl()
    api_sports_etl(39, 2023) # 39 is Premier League, 2023 season
    print_database()
    return


if __name__ == "__main__":
    main()
