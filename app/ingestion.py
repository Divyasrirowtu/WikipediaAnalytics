from pyspark.sql import DataFrame

def read_wikipedia_xml(spark, input_path: str) -> DataFrame:
    """
    Read Wikipedia XML data using spark-xml.
    """

    df = (
        spark.read
        .format("xml")
        .option("rowTag", "page")
        .load(input_path)
    )

    return df