from pyspark.sql import SparkSession


def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("WikipediaAnalytics")
        .config(
            "spark.jars.packages",
            "com.databricks:spark-xml_2.12:0.17.0"
        )
        .getOrCreate()
    )

    return spark