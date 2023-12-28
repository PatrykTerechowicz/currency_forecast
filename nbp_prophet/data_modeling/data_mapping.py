import yaml
from datetime import date, timedelta
from typing import Dict, List, Union
from pyspark.sql import SparkSession, DataFrame
from nbp_prophet.data_ingestion.nbp import NBPApi
import pyspark.sql.functions as F

"""
    Stworzymy mapowanie JSONOW z nbp.py do formatu czytanego przez pyspark.
    Stworzymy generator pobierajacy dane przy pomocy nbp.py.
"""


def map_response(response: List):
    return map(map_row, response)


def map_row(row: Dict):
    effectiveDate = row["effectiveDate"]
    rates = row["rates"]
    averages = {rate["code"]: rate["mid"] for rate in rates}
    averages.update({"effectiveDate": date.fromisoformat(effectiveDate)})
    return averages


def extract_data(spark: SparkSession, data_path: str):
    df = spark.read.parquet(data_path)
    return df


def get_last_update_date(df: DataFrame, start_date: date | str):
    try:
        last_date: str = df.select(F.max("effectiveDate")).first()[0]
    except:
        last_date = start_date
    if isinstance(last_date, str):
        last_date = date.fromisoformat(last_date)
    return last_date + timedelta(days=1)  # add 1 day so we don't fetch same data again


def update_data(spark: SparkSession, last_update_date: date, to_date: date | str):
    # If df is None then just return new_df
    api = NBPApi()
    new_data = api.fetch_since(last_update_date, to_date)
    mapped_new_data = map_response(new_data)
    new_df = spark.createDataFrame(mapped_new_data)
    return new_df


def load_data(df: DataFrame, data_path: str):
    # Saves data
    df.write.mode("append").parquet(data_path)
