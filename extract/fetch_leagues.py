import pandas as pd
import os
from dotenv import load_dotenv
import requests
import json

# load environment variables from .env
load_dotenv()

def fetch_leagues():
    """This function fetches leagues data from api and saves it to a json file"""
    api_key = os.getenv("API_KEY")

    url = "https://v3.football.api-sports.io/leagues"

    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': 'v3.football.api-sports.io'
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        os.makedirs("data/raw", exist_ok=True)
        with open("data/raw/leagues.json", "w") as f:
            json.dump(data, f, indent=2)
        print("Data saved to leagues.json")
    else:
        print(f"Error {response.status_code}: {response.text}")

