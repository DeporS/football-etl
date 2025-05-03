import pandas as pd


def extract_csv_to_dataframe(data_path: str):
    return pd.read_csv(data_path)

