import pandas as pd
import json
from datetime import datetime


def transform_leagues(raw_data: dict) -> pd.DataFrame:
    """
    This function transforms the raw data from dict read from a JSON file 
    into a pandas DataFrame, extracting relevant information about the leagues
    """
    leagues_data = raw_data["response"]

    leagues_list = []
    for league in leagues_data:
        for season in league["seasons"]:
                league_info = {
                    "league_id": league["league"]["id"],
                    "name" : league["league"]["name"],
                    "type": league["league"]["type"],
                    "country": league["country"]["name"],
                    "season": season["year"],
                    "start_date": season["start"],
                    "end_date": season["end"]
                }
                leagues_list.append(league_info)

    df = pd.DataFrame(leagues_list)

    # Additional transformations
    df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
    df["end_date"] = pd.to_datetime(df["end_date"]).dt.date
    df["duration_days"] = (pd.to_datetime(df["end_date"]) - pd.to_datetime(df["start_date"])).dt.days + 1

    return df