import database

import pandas as pd
from os import path, environ
import re
import glob
import logging
import logging_config
from datetime import datetime
from typing import Generator, Callable, Iterable

TranslatorType = Callable[[Iterable[str]], list[str]]

logging_config.config()
logger = logging.getLogger(__name__)

db = database.PrecipitationDB(
    host=environ['DB_HOST'],
    user=environ['DB_USER'],
    password=environ['DB_PASSWORD'],
    database = environ['DB_DATABASE'])


def read_precip_table(date: str, dir: str = 'data') -> pd.DataFrame:
    """
    Reads precipitation data from a CSV file.

    Args:
        date: date in ISO format
        dir: directory where the file is located

    Returns:
        Precipitation data.
    """
    file = path.join(dir, date) + '.csv'
    return pd.read_csv(file)


def write_precip_table(df: pd.DataFrame, date: str, dir: str ='data') -> None:
    """
    Saves precipitation data to a CSV file named by the date.

    Args:
        df: precipitation data
        date: date which will be used for the filename
        dir: directory where to save the file
    """
    file = path.join(dir, date) + '.csv'
    df.to_csv(file, index=False)


def stations_from_file(date: str) -> list[str]:
    """
    Reads stations from a precipitation file.

    Args:
        date: date whose file will be used

    Returns:
        List of stations.
    """
    df = read_precip_table(date)
    stations = sorted(list(df['Stanice']))
    return stations


def fill_stations_table() -> None:
    """
    Reads station data from a file and inserts it into a database table.
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


def get_station_name_translator() -> TranslatorType:
    """
    Creates a station name translator function.

    Returns:
        The translator
    """
    df = pd.read_csv('data/stations_data.csv')
    d = dict(zip(df['precip_known'], df['final']))

    return lambda names: [d[n] for n in names]


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


def convert_date(date: str) -> datetime:
    """
    Converts a date from string to datetime.

    Args:
        date: A date in dd.mm.yyyy or yyyy-mm-dd format.

    Returns:
        `date` as a datetime object.

    Raises:
        ValueError: If `date` is not in the correct format.

    Examples:
        >>> convert_date('2023-10-02')
        datetime.datetime(2023, 10, 2, 0, 0)

        >>> convert_date('2.10.2023')
        datetime.datetime(2023, 10, 2, 0, 0)

        >>> convert_date('32.10.2023')
        Traceback (most recent call last):
        ...
        ValueError: time data '32.10.2023' does not match format '%d.%m.%Y'
    """
    try:
        return datetime.fromisoformat(date)
    except ValueError:
        return datetime.strptime(date, '%d.%m.%Y')


def precip_table_to_long(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """
    Converts a one-day precipitation table from wide to long format

    Args:
        df: precipitation table in wide format (columns `Stanice` followed by `1`..`24`)
        date: day of measurements

    Returns:
        One-day precipitation table in long format with columns `station`,
            `amount` and `datetime`.
    """

    # reshape into three columns: Stanice, hour, precip
    long = pd.melt(df, id_vars=['Stanice'], var_name='hour', value_name='precip')
    long.rename(columns={'Stanice': 'station', 'precip': 'amount'}, inplace=True)
    # create 'datetime' column with time set to the middle of the previous hour (e.g. 22:30 for 23)
    long['datetime'] = convert_date(date) + pd.to_timedelta(
        long['hour'].astype(int) - .5, unit='hours')
    long['datetime'] = long['datetime'].astype(str)
    long.drop('hour', axis=1, inplace=True)

    return long


def precip_table_to_wide(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts a one-day precipitation table from long to wide format

    Args:
        df: one-day precipitation table in long format with columns `station`,
            `amount` and `datetime`.

    Returns:
        Precipitation table in wide format (columns `Stanice` followed by `1`..`24`)
    """
    df = df.copy()
    # extract hour as a new column (1..24) and drop 'datetime'
    df['hour'] = pd.DatetimeIndex(df['datetime']).hour + 1
    df.drop('datetime', axis=1, inplace=True)

    wide = df.pivot(index='station', columns='hour', values='amount')
    wide.columns = wide.columns.astype(str)
    wide.reset_index(inplace=True)
    wide.rename(columns={'station': 'Stanice'})

    return wide


def update_db_precipitation_from_dir(dir: str = 'data') -> None:
    """
    Updates the database from CSV files for dates not yet present there.

    Args:
        dir: directory to read files from
    """
    # find dates already present in the DB
    dates_in_db = db.get_precipitation_dates()
    dates_in_db = [str(d) for d in dates_in_db]

    # find CSV files for dates not yet present in the DB
    files = glob.glob(dir + '/*.csv')
    file_dates = [path.basename(f)[:-4] for f in files]  # just filename without extension
    file_dates_new = [fd for fd in file_dates
                      if re.fullmatch(r'\d{4}-\d{2}-\d{2}', fd)
                      and fd not in dates_in_db]

    update_db_precipitation_for_dates(file_dates_new)


def generate_data_for_dates(dates: list[str],
                            dir: str = 'data',
                            max_rows: int = 60000) -> Generator[pd.DataFrame, None, None]:
    """
    Creates a generator which successively returns precipitation data for given dates.

    Args:
        dates: dates for which data will be read from CSV files
        dir: location of the files
        max_rows: maximum number of rows in one batch (yield)

    Returns:
        Data generator.
    """
    df_batch = None  # used to accumulate data for one batch
    for date in dates:
        df = read_precip_table(date, dir)
        df = precip_table_to_long(df, date)

        if df.shape[0] > max_rows:
            message = (f'Data for {date} has more rows ({df.shape[0]}) '
                       f'than allowed by `max_rows` ({max_rows}). '
                       f'Consider increasing `max_rows`.')
            logger.error(message)
            raise RuntimeError(message)

        if df_batch is None:
            df_batch = df
        else:
            # concatenate dataframes when max_rows is not exceeded
            if (df_batch.shape[0] + df.shape[0]) <= max_rows:
                df_batch = pd.concat([df_batch, df], sort=False)
            # insert batch into DB and start a new one
            else:
                yield df_batch
                df_batch = df

    # insert what is left after the last loop run
    if df_batch is not None:
        yield df_batch


def update_db_precipitation_for_dates(dates: list[str],
                                      dir: str = 'data',
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
    data = generate_data_for_dates(dates=dates, dir=dir, max_rows=max_rows)
    db.insert_precip_data(data)

# db.create_tables()
# fill_stations_table()

# update_db_precipitation_from_dir()
