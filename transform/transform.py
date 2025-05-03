import pandas as pd


def transform_data(data: pd.DataFrame):
    referees = data.groupby('Referee').agg({
        'HY': 'mean',
        'AY': 'mean',
        'HR': 'mean',
        'AR': 'mean'
    }).round(2)

    referees['avg_yellow_cards'] = referees['HY'] + referees['AY']
    referees['avg_red_cards'] = referees['HR'] + referees['AR']

    # drop temporary columns
    referees = referees.drop(['HY', 'AY', 'HR', 'AR'], axis=1)

    # move Referee from index to column
    referees = referees.reset_index()

    # change Referee column name
    referees = referees.rename(columns={'Referee': 'referee_name'})

    return referees
