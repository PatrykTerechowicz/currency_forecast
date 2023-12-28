import nbp_prophet.utils.date_utils as date_utils
import datetime
import math


def test_split_date_range_split_lengths():
    start_date = datetime.date.fromisoformat("2023-01-01")
    end_date = datetime.date.fromisoformat("2023-12-31")

    for split_length in range(1, 369):
        split = date_utils.split_date_range(
            start_date, end_date, datetime.timedelta(days=split_length)
        )
        for d1, d2 in split:
            assert (d2 - d1).days <= split_length


def test_split_date_range_same_date():
    start_date = datetime.date.fromisoformat("2023-01-01")

    split = date_utils.split_date_range(start_date, start_date)
    assert split == [(start_date, start_date)]
