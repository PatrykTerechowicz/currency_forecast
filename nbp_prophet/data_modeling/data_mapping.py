import yaml
from datetime import date, timedelta
from typing import Dict, List, Union
import pyspark.sql as ps
from nbp_prophet.data_ingestion.nbp import NBPApi
import pyspark.sql.functions as F

"""
    Our DataSchema contains column effectiveDate which is date type,
    other columns are named by currency's ISO 4217 code and are float type.
"""


def map_response(response: List) -> List[Dict]:
    return list(map(map_row, response))


def map_row(row: Dict) -> Dict:
    effectiveDate = row["effectiveDate"]
    rates = row["rates"]
    averages = {rate["code"]: rate["mid"] for rate in rates}
    averages.update({"effectiveDate": date.fromisoformat(effectiveDate)})
    return averages


def extract_data(spark: ps.SparkSession, data_path: str) -> ps.DataFrame:
    """
    Loads DataFrame from given data_path.
    """
    df = spark.read.parquet(data_path)
    return df


def get_last_update_date(df: ps.DataFrame, start_date: date | str) -> date:
    """
    Will return most recent effectiveDate in given DataFrame,
    if DataFrame is empty then returns start_date.
    """
    try:
        last_date: str = df.select(F.max("effectiveDate")).first()[0]
    except:
        last_date = start_date
    if isinstance(last_date, str):
        last_date = date.fromisoformat(last_date)
    return last_date + timedelta(days=1)  # add 1 day so we don't fetch same data again


def get_updated_data(
    spark: ps.SparkSession, last_update_date: date, to_date: date | str
) -> ps.DataFrame:
    """
    Fetches data from NBP Api, maps it and returns DataFarme with updated data.
    """
    # If df is None then just return new_df
    api = NBPApi()
    new_data = api.fetch_since(last_update_date, to_date)
    mapped_new_data = map_response(new_data)
    new_df = spark.createDataFrame(mapped_new_data)
    return new_df


def load_data(df: ps.DataFrame, data_path: str):
    """
    Appends DataFrame df into data_path in parquet.
    """
    df.write.mode("append").parquet(data_path)


def update_data():
    ...
