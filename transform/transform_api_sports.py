import pandas as pd
import json

pd.set_option('display.max_columns', None)


def load_raw_json(filepath: str) -> dict:
    with open(filepath, "r") as f:
        return json.load(f)
    
def transform_standings(raw_data: dict) -> pd.DataFrame:
    """
    This function transformes the raw data from dict read from a JSON file 
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

def transform_fixtures():

    return