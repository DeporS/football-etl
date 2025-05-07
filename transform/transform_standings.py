import pandas as pd
import json
from datetime import datetime


def transform_standings(raw_data: dict) -> pd.DataFrame:
    """
    This function transforms the raw data from dict read from a JSON file 
    into a pandas DataFrame, extracting relevant information about the teams
    """
    standings_data = raw_data["response"][0]["league"]["standings"][0]
    
    standing_teams = []
    for team in standings_data:
        team_info = {
            "country": raw_data["response"][0]["league"]["country"],
            "league": raw_data["response"][0]["league"]["name"],
            "rank": team["rank"],
            "team": team["team"]["name"],
            "points": team["points"],
            "wins": team["all"]["win"],
            "draws": team["all"]["draw"],
            "losses": team["all"]["lose"],
            "goals_scored": team["all"]["goals"]["for"],
            "goals_conceded": team["all"]["goals"]["against"]
        }
        standing_teams.append(team_info)
    
    df = pd.DataFrame(standing_teams)

    # additional transformations
    df['points_per_game'] = (df['points'] / (df['wins'] + df['draws'] + df['losses'])).round(2)
    df['goal_difference'] = df['goals_scored'] - df['goals_conceded']

    return df