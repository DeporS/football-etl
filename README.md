# football-etl

Extracting, Transforming and Loading football data in Python.

## Project Structure

### `main.py`

In main we can launch ETL processes.

### `referees_etl`

- **Extract (E) -** Extracting data from csv file
- **Transform (T) -** Applying transformations to calculate average yellow and red cards between referees in `transform_referees_data` function
- **Load (L) -** Loading the transformed data into a `referees` table in PostgreSQL database (running in Docker) using `load_data_to_referees` function

### `api_sports_etl`

- **Extract (E) -** Extracting data from API endpoints using the functions `fetch_leagues`, `fetch_standings` and `fetch_fixtures`
- **Transform (T) -** Transforming raw data from `.json` files into structured DataFrames, preparing them to loading into the database using functions `transform_leagues`, `transform_standings` and `transform_fixtures`
- **Load (L) -**

## Database Structure

![Database Structure](database_struct.png)
