"""
Functions for reading and writing CSV files and converting between long
and wide data formats.
"""

import logging_config
from utils import *

from os import path
import pandas as pd
import glob
import re
from typing import Generator

# CSV files location
CSV_DIR = 'data/daily'

logger = logging_config.get_local_logger(__name__)


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


def read_precip_table(date: str, dir: str = CSV_DIR) -> pd.DataFrame:
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


def write_precip_table(df: pd.DataFrame, date: str, dir: str = CSV_DIR) -> None:
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


def get_csv_dates(dir: str = CSV_DIR) -> list[str]:
    """
    Returns measurement dates of saved CSV files.

    Args:
        dir: directory where the files are located

    Returns:
        List of dates in ISO format.
    """
    files = glob.glob(dir + '/*.csv')
    # remove extensions
    file_names = [path.basename(f)[:-4] for f in files]
    return [n for n in file_names if re.fullmatch(r'\d{4}-\d{2}-\d{2}', n)]


def provide_data_for_dates(dates: list[str],
                           dir: str = CSV_DIR,
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