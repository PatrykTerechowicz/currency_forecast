from nbp_prophet.data_ingestion.nbp import NBPApi
from datetime import datetime, date


def json_tester(response):
    for row in response:
        assert "effectiveDate" in row
        assert "rates" in row
        for rate in row["rates"]:
            assert "code" in rate
            assert "currency" in rate
            assert "mid" in rate


def test_fetch_averages():
    api = NBPApi()
    ### 2023
    first_day = date.fromisoformat("2023-01-01")
    last_day = date.fromisoformat("2023-01-14")
    response = api.fetch_averages(first_day, last_day)

    json_tester(response)

    ### 2022
    first_day = date.fromisoformat("2022-01-01")
    last_day = date.fromisoformat("2022-01-14")
    response = api.fetch_averages(first_day, last_day)

    json_tester(response)

    ### 2021
    first_day = date.fromisoformat("2021-01-01")
    last_day = date.fromisoformat("2021-01-14")
    response = api.fetch_averages(first_day, last_day)

    json_tester(response)


def test_fetch_since():
    api = NBPApi()
    since_date = date.fromisoformat("2023-10-01")
    response = api.fetch_since(since_date, date.today())
    json_tester(response)

    for row in response:
        assert date.fromisoformat(row["effectiveDate"]) >= since_date


def test_fetch_since_str():
    api = NBPApi()
    response = api.fetch_since("2023-01-01", "2023-01-10")
    json_tester(response)

    response = api.fetch_since(date.fromisoformat("2023-01-01"), "2023-01-10")
    json_tester(response)

    response = api.fetch_since("2023-01-01", date.fromisoformat("2023-01-10"))
    json_tester(response)
