"""
Functions for downloading and parsing precipitation data.
"""

import utils
import logging_config

import bs4
from bs4 import BeautifulSoup
import requests
import re
import time
from datetime import date, datetime
import pandas as pd

logger = logging_config.get_download_logger(__name__)


def extract_n_date(page: BeautifulSoup) -> tuple[int, datetime]:
    """
    Extracts the total number of pages and the date precipitation data is for.

    Args:
        page: parsed page

    Returns:
        A tuple containing:
            1. total number of pages
            2. measurement date
    """
    # extract the total number of pages
    n_str = page.find('div', string=re.compile('Celkov')).string.strip()
    n = int(re.search('[0-9]+$', n_str).group())

    # extract date
    dat = page.find('th', string=re.compile('^Datum')).string
    dat = re.search(r'[0-9]+\.[0-9]+\.[0-9]{4}$', dat).group()
    dat = utils.convert_date(dat)

    return n, dat


def download_precip_offset(day_offset: int = 0) -> tuple[pd.DataFrame, datetime]:
    """
    Downloads precipitation data for a given day offset.

    Args:
        day_offset: day offset value

    Returns:
        A tuple containing:
            1. precipitation data frame in wide format
            2. measurement day
    """
    logger.info(f'Starting to download first page for day offset {day_offset}')
    page = download_page(day_offset=day_offset)

    n, dat = extract_n_date(page)
    logger.info(f'{n} total pages to be downloaded for {dat.date().isoformat()}')

    # concatenate all tables for the current day
    data = []
    for i in range(n):
        if i > 0:
            page = download_page(day_offset=day_offset, subpage=i + 1)
        # extract precipitation table
        table = page.select_one('div.tsrz table')
        data += (read_precip_table(table, include_header=i == 0))
        time.sleep(.5)

    df = pd.DataFrame(data[1:], columns=data[0])

    # keep only 'Stanice' and '1'..'24' columns and remove others
    to_keep = ['Stanice'] + [str(h+1) for h in range(24)]
    to_drop = [col for col in df.columns if col not in to_keep]
    df.drop(to_drop, axis=1, inplace=True)

    return df, dat


def download_precip_date(dat: str, allow_today: bool = False) -> pd.DataFrame:
    """
    Downloads precipitation data for the specified day.

    Args:
        dat: precipitation day
        allow_today: should today's date be allowed (data won't be complete)

    Returns:
        Data for the date.
    """
    offset = date.today() - date.fromisoformat(dat)
    offset = offset.days
    offset_min = 0 if allow_today else 1
    offset_max = 7

    if offset < offset_min or offset > offset_max:
        logger.error(f'offset ({offset}) for date {dat} outside allowed range: {offset_min}-{offset_max}')
        raise ValueError(f'Date {dat} outside allowed range.')

    df, date_df = download_precip_offset(offset)

    # in case something weird happened
    if utils.convert_date(dat) != date_df:
        message = f'Downloaded precipitation date {date_df.date().isoformat()} is different than requested {dat}; offset: {offset}.'
        logger.error(message)
        raise RuntimeError(message)

    return df


def read_precip_table(table: bs4.Tag, include_header: bool = False) -> list[list]:
    """
    Reads precipitation data from an HTML table.

    Args:
        table: an HTML ``table`` element
        include_header: Should table column names be read and returned?

    Returns:
        Table as a list of lists
    """
    data = []
    # extract table's column names
    if include_header:
        data = [list(table.tr.stripped_strings)]
    # read table data
    for row in table.select('tr')[1:]:
        data.append([next(cell.stripped_strings, None) for cell in row.select('td')])

    return data


def download_page(day_offset: int = 0, subpage: int = 1) -> BeautifulSoup:
    """
    Downloads and parses precipitation for a given day offset.

    Args:
        day_offset: day offset value (0: today, 1: yesterday, ...,
            7 is the maximum supported by the data source)
        subpage: subpage number

    Returns:
        Parsed page.
    """
    url = f'https://hydro.chmi.cz/hppsoldv/hpps_act_rain.php?day_offset={day_offset}&startpage={subpage}'
    resp = requests.get(url)
    return BeautifulSoup(resp.content, 'html.parser')


# dd = str(date.today())
# df = download_precip_date(dd)
# df, dat = download_precip_offset(day_offset=2)
# df.to_csv('data/' + dat.isoformat() + '.csv', index=False)
