from datetime import date, datetime, timedelta


def split_date_range(
    start_date: date,
    end_date: date,
    max_length: timedelta = timedelta(days=93),
):
    ranges = []
    next_date = start_date + max_length
    while next_date < end_date:
        ranges.append((start_date, next_date))
        start_date = next_date + timedelta(days=1)
        next_date = start_date + max_length
    ranges.append((start_date, end_date))
    return ranges
