import pandas as pd
import os
from dotenv import load_dotenv
import requests
import json

# load environment variables from .env
load_dotenv()

def fetch_fixtures(league_id: int, season: int):
    """This function fetches fixtures for the provided league and season then saves it to a json file"""
    api_key = os.getenv("API_KEY")

    url = f"https://v3.football.api-sports.io/fixtures?league={league_id}&season={season}"

    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': 'v3.football.api-sports.io'
    }

    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        os.makedirs("data/raw", exist_ok=True)
        with open(f"data/raw/fixtures_{league_id}_{season}.json", "w") as f:
            json.dump(data, f, indent=2)
        print(f"Data saved to fixtures_{league_id}_{season}.json")
    else:
        print(f"Error {response.status_code}: {response.text}")

