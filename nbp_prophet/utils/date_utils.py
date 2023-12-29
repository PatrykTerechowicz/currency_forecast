from datetime import date, datetime, timedelta
from typing import List, Tuple


def split_date_range(
    start_date: date,
    end_date: date,
    max_length: timedelta = timedelta(days=93),
) -> List[Tuple[date, date]]:
    ranges = []
    next_date = start_date + max_length
    while next_date < end_date:
        ranges.append((start_date, next_date))
        start_date = next_date + timedelta(days=1)
        next_date = start_date + max_length
    ranges.append((start_date, end_date))
    return ranges
