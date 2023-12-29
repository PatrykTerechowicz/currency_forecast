import shutil
import os
from nbp_prophet.data_modeling import data_mapping
from nbp_prophet.data_ingestion.nbp import NBPApi
from datetime import date, timedelta
from pyspark.sql import SparkSession

api = NBPApi()

start_date = date.fromisoformat("2023-01-01")
end_date = date.fromisoformat("2023-01-31")
response = api.fetch_averages(start_date, end_date)


def clean_test_data():
    if os.path.exists("tests/data.parquet"):
        shutil.rmtree("tests/data.parquet")


def test_map_row():
    mapped = data_mapping.map_row(response[0])
    assert "effectiveDate" in mapped


def test_map_response():
    mapped = data_mapping.map_response(response)

    for row in mapped:
        assert "effectiveDate" in row


def test_mapping_pipeline_sanity_check():
    # If it doesn't fail then we consider test passed
    clean_test_data()
    data_path = "./tests/data.parquet"
    since_date: date = date.fromisoformat("2023-01-01")

    spark: SparkSession = (
        SparkSession.builder.master("local").appName("Data Mapping Test").getOrCreate()
    )
    try:
        df = data_mapping.extract_data(spark, data_path)
        last_update_date: date = data_mapping.get_last_update_date(df, since_date)
    except:
        df = None  # if couldn't read then just set it to None
        last_update_date = since_date
    new_df = data_mapping.get_updated_data(spark, last_update_date, date.today())
    data_mapping.load_data(new_df, data_path)

    spark.stop()


def test_mapping_pipeline_multiple():
    """
    This tests running mapping pipeline in monthly basis.
    """
    clean_test_data()
    data_path = "./tests/data.parquet"
    since_date: date = date.fromisoformat("2023-01-01")

    spark: SparkSession = (
        SparkSession.builder.master("local").appName("Data Mapping batch").getOrCreate()
    )

    def pipeline(current_date: date):
        try:
            df = data_mapping.extract_data(spark, data_path)
            last_update_date: date = data_mapping.get_last_update_date(df, since_date)
        except:
            df = None  # if couldn't read then just set it to None
            last_update_date = since_date
        new_df = data_mapping.get_updated_data(spark, last_update_date, current_date)
        data_mapping.load_data(new_df, data_path)

    dates = list(map(date.fromisoformat, ["2023-02-01", "2023-03-01", "2023-04-01"]))
    pipeline(dates[0])
    pipeline(dates[1])
    pipeline(dates[2])

    spark.stop()
