import pandas as pd
import json
from datetime import datetime

pd.set_option('display.max_columns', None)


def load_raw_json(filepath: str) -> dict:
    with open(filepath, "r") as f:
        return json.load(f)
    
def transform_standings(raw_data: dict) -> pd.DataFrame:
    """
    This function transforms the raw data from dict read from a JSON file 
    into a pandas DataFrame, extracting relevant information about the teams
    """
    standings_data = raw_data["response"][0]["league"]["standings"][0]
    
    standing_teams = []
    for team in standings_data:
        team_info = {
            "rank": team["rank"],
            "team": team["team"]["name"],
            "points": team["points"],
            "wins": team["all"]["win"],
            "draws": team["all"]["draw"],
            "loses": team["all"]["lose"],
            "goals_scored": team["all"]["goals"]["for"],
            "goals_conceded": team["all"]["goals"]["against"]
        }
        standing_teams.append(team_info)
    
    df = pd.DataFrame(standing_teams)

    # additional transformations
    df['points_per_game'] = (df['points'] / (df['wins'] + df['draws'] + df['loses'])).round(2)
    df['goal_difference'] = df['goals_scored'] - df['goals_conceded']

    return df


# data = load_raw_json("data/raw/standings_39_2023.json")
# print(transform_standings(data))

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

data2 = load_raw_json("data/raw/fixtures_39_2023.json")
print(transform_fixtures(data2))