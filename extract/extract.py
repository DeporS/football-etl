import pandas as pd
from dotenv import load_dotenv
import json

# load environment variables from .env
load_dotenv()


def extract_csv_to_dataframe(data_path: str):
    return pd.read_csv(data_path)


def load_raw_json(filepath: str) -> dict:
    with open(filepath, "r") as f:
        return json.load(f)