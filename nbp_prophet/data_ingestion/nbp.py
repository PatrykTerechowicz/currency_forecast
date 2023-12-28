import sys
import yaml
import json
import requests
from datetime import date, datetime, timedelta
from nbp_prophet.utils.date_utils import split_date_range


class NBPApi:
    def __init__(self) -> None:
        self.endpoint = ""
        self._load_config()

    def _load_config(self):
        with open("./config/nbp_api.yaml") as config_file:
            config = yaml.safe_load(config_file)
            self.endpoint = config["endpoint"]

    def fetch_averages(self, start_date: date, end_date: date):
        assert (
            end_date >= start_date
        ), f"end_date ({end_date}) must be after or equal start_date ({start_date})"
        assert (
            end_date - start_date
        ).days <= 93, f"too long period (can't be longer than 93 days)"
        table = "a"
        url = (
            self.endpoint + f"{table}/{start_date.isoformat()}/{end_date.isoformat()}/"
        )
        res = requests.get(url)
        if res.status_code != 200:
            return []
        response = json.loads(res.text)
        return response

    def fetch_since(self, since_date: date | str, to_date: date | str):
        if isinstance(since_date, str):
            since_date = date.fromisoformat(since_date)
        if isinstance(to_date, str):
            to_date = date.fromisoformat(to_date)
        assert since_date <= to_date

        date_ranges = split_date_range(since_date, to_date)
        response = []
        for d1, d2 in date_ranges:
            response.extend(self.fetch_averages(d1, d2))
        return response
