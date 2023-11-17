import bs4
from bs4 import BeautifulSoup
import requests
import re
import time
from datetime import date, datetime
import pandas as pd
import logging_config
import logging
import data_manip as dm

logging_config.config()
logger = logging.getLogger(__name__)


def extract_n_date(page: BeautifulSoup) -> tuple[int, str]:
    # extract the total number of pages
    n_str = page.find('div', string=re.compile('Celkov')).string.strip()
    n = int(re.search('[0-9]+$', n_str).group())

    # extract date
    date = page.find('th', string=re.compile('^Datum')).string
    date = re.search(r'[0-9]+\.[0-9]+\.[0-9]{4}$', date).group()
    date = datetime.strptime(date, '%d.%m.%Y').strftime('%Y-%m-%d')

    return n, date


def download_precip_offset(day_offset: int = 0) -> tuple[pd.DataFrame, str]:
    logger.info(f'Starting to download first page for day offset {day_offset}')
    page = download_page(day_offset=day_offset)

    n, date = extract_n_date(page)
    logger.info(f'{n} total pages to be downloaded for {date}')

    # concatenate all tables for the current day
    data = []
    for i in range(n):
        if i > 0:
            page = download_page(day_offset=day_offset, page=i+1)
        # extract precipitation table
        table = page.select_one('div.tsrz table')
        data += (read_precip_table(table, include_header=i == 0))
        time.sleep(1)

    df = pd.DataFrame(data[1:], columns=data[0])

    # keep only 'Stanice' and '1'..'24' columns and remove others
    to_keep = ['Stanice'] + [str(h+1) for h in range(24)]
    to_drop = [col for col in df.columns if col not in to_keep]
    df.drop(to_drop, axis=1, inplace=True)

    return df, date


def download_precip_date(dat: str, allow_today: bool = False) -> pd.DataFrame:
    offset = date.today() - date.fromisoformat(dat)
    offset = offset.days
    offset_min = 0 if allow_today else 1
    offset_max = 7

    if offset < offset_min or offset > offset_max:
        logger.error(f'offset ({offset}) for date {dat} outside allowed range: {offset_min}-{offset_max}')
        raise ValueError(f'Date {dat} outside allowed range.')

    df, date_df = download_precip_offset(offset)

    if dm.convert_date(dat) != dm.convert_date(date_df):
        logger.error(f'Downloaded precipitation date {date_df} is different than requested {dat}; offset: {offset}.')
        raise RuntimeError(f'Downloaded data is for different date {date_df} than expected {dat}.')

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


def download_page(day_offset: int = 0, page: int = 1) -> BeautifulSoup:
    url = f'https://hydro.chmi.cz/hppsoldv/hpps_act_rain.php?day_offset={day_offset}&startpage={page}'
    resp = requests.get(url)
    soup = BeautifulSoup(resp.content, 'html.parser')
    return soup


# dd = str(date.today())
# df = download_precip_date(dd)
df, dat = download_precip_offset(day_offset=2)
df.to_csv('data/' + dat + '.csv', index=False)

