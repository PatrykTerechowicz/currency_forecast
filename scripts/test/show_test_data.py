from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark: SparkSession = (
        SparkSession.builder.master("local").appName("Show test data").getOrCreate()
    )
    df = spark.read.parquet("tests/data.parquet")
    print(df.select("*").where("effectiveDate > '2023-03-01'").head(1))
    df.printSchema()
