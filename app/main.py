import os
from dotenv import load_dotenv

from spark_session import create_spark_session
from ingestion import read_wikipedia_xml


def main():

    load_dotenv(".env.example")

    spark = create_spark_session()

    input_path = os.getenv("INPUT_PATH")

    df = read_wikipedia_xml(spark, input_path)

    print("\n===== SCHEMA =====")
    df.printSchema()

    print("\n===== SAMPLE DATA =====")
    df.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()