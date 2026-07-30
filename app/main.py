import os
from dotenv import load_dotenv

from spark_session import create_spark_session
from ingestion import read_wikipedia_xml
from transform import transform_wikipedia_data


def main():

    load_dotenv(".env.example")

    spark = create_spark_session()

    input_path = os.getenv("INPUT_PATH")

    raw_df = read_wikipedia_xml(
        spark,
        input_path
    )

    transformed_df = transform_wikipedia_data(raw_df)

    print("\n==============================")
    print("TRANSFORMED SCHEMA")
    print("==============================")

    transformed_df.printSchema()

    print("\n==============================")
    print("TRANSFORMED DATA")
    print("==============================")

    transformed_df.show(
        truncate=False
    )

    spark.stop()


if __name__ == "__main__":
    main()