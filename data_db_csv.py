"""
Provides functions which connect the database with CSV files or further process
data retrieved from the database.
"""

from utils import *
import database
import data_csv as csv
import credentials

import pandas as pd
import logging
import logging_config


logging_config.config()
logger = logging.getLogger(__name__)

db = database.PrecipitationDB(
    host=credentials.DB_HOST,
    user=credentials.DB_USER,
    password=credentials.DB_PASSWORD,
    database=credentials.DB_DATABASE)


def fill_stations_table() -> None:
    """
    Reads station data from a file and inserts it into a database table.
    NOT SUPPOSED TO BE USED at the moment.
    """
    stations = pd.read_csv('data/stations_data.csv')
    db.insert_stations(stations)


def get_stations_data(name_translator: TranslatorType = None) -> pd.DataFrame:
    """
    Reads station data from the database, translating their names when
        a translator is given.

    Args:
        name_translator: function for name translation

    Returns:
        Stations data
    """
    df = db.get_stations_data()
    df['elevation'] = df['elevation'].astype(int)
    if name_translator is not None:
        df['name'] = name_translator(df['name'])
    df.set_index('name', inplace=True, drop=False)

    return df


def get_daily_precipitation(station_translator: TranslatorType = None) -> pd.DataFrame:
    """
    Retrieves daily precipitation data by station from the database.

    Args:
        station_translator: station name translator

    Returns:
        Daily precipitation data for every station.
    """
    df = db.get_daily_precipitation()
    if station_translator is not None:
        df['station'] = station_translator(df['station'])
    df['station'] = df['station'].astype('category')
    df['amount'] = df['amount'].astype(float)
    df['date'] = pd.to_datetime(df['date'])

    # create a multiindex from 'station' and 'date' naming the levels 'station_idx' and 'date_idx'
    df.rename(columns={'station': 'station_idx', 'date': 'date_idx'}, inplace=True)
    df.set_index(['station_idx', 'date_idx'], inplace=True, drop=False)
    df.rename(columns={'station_idx': 'station', 'date_idx': 'date'}, inplace=True)

    return df


def update_db_precipitation_from_dir(dir: str = csv.CSV_DIR) -> None:
    """
    Updates the database from CSV files for dates not yet present there.

    Args:
        dir: directory to read files from
    """

    # find dates already present in the DB
    dates_db = db.get_precipitation_dates()
    dates_db = [str(d) for d in dates_db]

    # find CSV files for dates not yet present in the DB
    dates_csv = csv.get_csv_dates(dir)
    dates_csv_new = [d for d in dates_csv if d not in dates_db]

    update_db_precipitation_for_dates(dates_csv_new)


def update_db_precipitation_for_dates(dates: list[str],
                                      dir: str = csv.CSV_DIR,
                                      max_rows: int = 60000) -> None:
    """
    Updates the database from CSV files for given dates.

    Args:
        dates: dates to be added in the database
        dir: CSV file location
        max_rows: maximum number of rows inserted at once
    """
    if len(dates) == 0:
        logger.info(f'Database update requested but 0 dates given.')
        return

    logger.info(f'Data will be inserted into database for {len(dates)} date(s): {", ".join(dates)}')
    data = csv.provide_data_for_dates(dates=dates, dir=dir, max_rows=max_rows)
    db.insert_precip_data(data)


def get_station_name_translator() -> TranslatorType:
    """
    Creates a station name translator function.

    Returns:
        The translator
    """
    df = pd.read_csv('data/stations_data.csv')
    d = dict(zip(df['precip_known'], df['final']))

    return lambda names: [d[n] for n in names]


# db.create_tables()
# fill_stations_table()

# update_db_precipitation_from_dir()
