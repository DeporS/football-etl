import pandas as pd

from load.load_competitions import load_data_to_competitions
from load.load_competition_seasons import load_data_to_competition_seasons
from load.load_standings import load_data_to_standings
from load.load_teams import load_data_to_teams
from load.load_team_stats import load_data_to_team_stats
from load.load_matches import load_data_to_matches
from load.load_match_stats import load_data_to_match_stats

def load_data(season: int, league_id: int):
    load_data_to_competitions(pd.read_csv("data/transformed/transformed_leagues.csv"))
    load_data_to_competition_seasons(pd.read_csv("data/transformed/transformed_leagues.csv"))
    st_id = load_data_to_standings(pd.read_csv(f"data/transformed/transformed_standings_{league_id}_{season}.csv"), season)
    load_data_to_teams(pd.read_csv(f"data/transformed/transformed_standings_{league_id}_{season}.csv"), st_id)
    load_data_to_team_stats(pd.read_csv(f"data/transformed/transformed_standings_{league_id}_{season}.csv"), st_id)
    cs_id = load_data_to_matches(pd.read_csv(f"data/transformed/transformed_fixtures_{league_id}_{season}.csv"), season)
    load_data_to_match_stats(pd.read_csv(f"data/transformed/transformed_fixtures_{league_id}_{season}.csv"), cs_id)

