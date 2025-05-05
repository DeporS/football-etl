import pandas as pd
import json
from datetime import datetime


def transform_fixtures(raw_data: dict) -> pd.DataFrame:
    """
    This function transforms the raw data from dict read from a JSON file 
    into a pandas DataFrame, extracting relevant information about the fixtures
    """
    fixtures_data = raw_data["response"]
    
    fixtures_list = []
    for fixture in fixtures_data:

        raw_date = fixture["fixture"]["date"]
        dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
        
        fixture_info = {
            "league": fixture["league"]["name"],
            "date_utc": dt.strftime("%Y-%m-%d %H:%M:%S"),
            # "year": dt.year,
            # "month": dt.month,
            # "day": dt.day,
            # "time": dt.strftime("%H:%M")
            "home_team": fixture["teams"]["home"]["name"],
            "away_team": fixture["teams"]["away"]["name"],
            "goals_home": fixture["goals"]["home"],
            "goals_away": fixture["goals"]["away"],
            "referee": fixture["fixture"]["referee"],
            "home_halftime_outcome": "win" if fixture["score"]["halftime"]["home"] > fixture["score"]["halftime"]["away"] 
                    else "lose" if fixture["score"]["halftime"]["home"] < fixture["score"]["halftime"]["away"] else "draw",
            "home_outcome": "win" if fixture["score"]["fulltime"]["home"] > fixture["score"]["fulltime"]["away"] 
                    else "lose" if fixture["score"]["fulltime"]["home"] < fixture["score"]["fulltime"]["away"] else "draw",
        }
        fixtures_list.append(fixture_info)
        
    df = pd.DataFrame(fixtures_list)

    # additional transformations
    df["reversed_result"] = df.apply(
        lambda row: row["home_halftime_outcome"] != "draw" 
        and row["home_outcome"] != "draw" 
        and row["home_halftime_outcome"] != row["home_outcome"],
        axis=1
    )

    return df