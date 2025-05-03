import pandas as pd
import os
from dotenv import load_dotenv
import http.client

# load environment variables from .env
load_dotenv()


def extract_csv_to_dataframe(data_path: str):
    return pd.read_csv(data_path)


def extract_data_api_sports():
    api_key = os.getenv("API_KEY")

    conn = http.client.HTTPSConnection("v3.football.api-sports.io")

    headers = {
        'x-rapidapi-key': f'{api_key}',
        'x-rapidapi-host': 'v3.football.api-sports.io'
    }
    conn.request("GET", "/teams/statistics?season=2023&team=33&league=39", headers=headers)

    res = conn.getresponse()
    data = res.read()

    print(data.decode("utf-8"))

# extract_data_api_sports()

def extract_league_data_api_sports():
    api_key = os.getenv("API_KEY")

    conn = http.client.HTTPSConnection("v3.football.api-sports.io")

    headers = {
        'x-rapidapi-key': f'{api_key}',
        'x-rapidapi-host': 'v3.football.api-sports.io'
    }
    conn.request("GET", "/leagues?id=39&season=2023", headers=headers)

    res = conn.getresponse()
    data = res.read()

    print(data.decode("utf-8"))

extract_league_data_api_sports()