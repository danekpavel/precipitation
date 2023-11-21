from datetime import datetime
from typing import Callable, Iterable

TranslatorType = Callable[[Iterable[str]], list[str]]

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
