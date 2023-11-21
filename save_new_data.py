from download import download_precip_date
import data_csv as csv
import logging_config

from datetime import date, timedelta
import logging


logging_config.config()
logger = logging.getLogger(__name__)


def save_new_data(min_offset: int = 1, max_offset: int = 7) -> None:
    """
    Downloads and saves as CSV data for dates not downloaded yet.

    Args:
        min_offset: the most recent day to download; 0: today,
            1: yesterday, ...
        max_offset: the oldest day to download
    """
    dates_csv = csv.get_csv_dates()
    # offset-specified dates in ISO format
    dates_recent = [(date.today() - timedelta(days=d)).isoformat()
                    for d in range(min_offset, max_offset + 1)]
    # dates to be downloaded
    dates_new = set(dates_recent) - set(dates_csv)

    dates_merged = '(' + ', '.join(dates_new) + ') ' if len(dates_new) else ''
    logger.info(f'Precipitation data for {len(dates_new)} new dates {dates_merged}'
                f'to be downloaded between offsets [{min_offset}, {max_offset}].')

    for dat in dates_new:
        df = download_precip_date(dat, allow_today=True)
        csv.write_precip_table(df, dat)


if __name__ == 'main':
    save_new_data()

