# football-etl

Extracting, Transforming and Loading football data in Python.

## Project Structure

### `main.py`

In main we can launch ETL processes.

### `referees_etl`

- **Extract (E) -** Extracting data from csv file
- **Transform (T) -** Applying transformations to calculate average yellow and red cards between referees
- **Load (L) -** Loading the transformed data into a PostgreSQL database (running in Docker)

### `api_sports_etl`

- **Extract (E) -** Extracting data from API endpoints using the functions `fetch_leagues`, `fetch_standings` and `fetch_fixtures`
- **Transform (T) -**
- **Load (L) -**

### [Database Structure](database_struct.png)
