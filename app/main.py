import os

from dotenv import load_dotenv

from spark_session import create_spark_session
from ingestion import read_wikipedia_xml
from transform import transform_wikipedia_data

from analysis import (
    edit_volume_per_hour,
    revert_rate_per_editor,
    contested_articles
)


def main():

    load_dotenv(".env.example")

    spark = create_spark_session()

    input_path = os.getenv("INPUT_PATH")
    output_path = os.getenv("OUTPUT_PATH")

    # Read XML
    raw_df = read_wikipedia_xml(
        spark,
        input_path
    )

    # Transform Data
    transformed_df = transform_wikipedia_data(
        raw_df
    )

    print("=" * 60)
    print("TRANSFORMED DATA")
    print("=" * 60)

    transformed_df.show(
        truncate=False
    )

    # ------------------------------------
    # Analysis 1
    # ------------------------------------

    edit_volume_df = edit_volume_per_hour(
        transformed_df
    )

    print("=" * 60)
    print("EDIT VOLUME PER ARTICLE PER HOUR")
    print("=" * 60)

    edit_volume_df.show(
        truncate=False
    )

    (
        edit_volume_df.write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(
            f"{output_path}/edit_volume.parquet"
        )
    )

    # ------------------------------------
    # Analysis 2
    # ------------------------------------

    revert_rate_df = revert_rate_per_editor(
        transformed_df
    )

    print("=" * 60)
    print("REVERT RATE PER EDITOR")
    print("=" * 60)

    revert_rate_df.show(
        truncate=False
    )

    (
        revert_rate_df.write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(
            f"{output_path}/revert_rate.parquet"
        )
    )

    # ------------------------------------
    # Analysis 3
    # ------------------------------------

    contested_df = contested_articles(
        transformed_df
    )

    print("=" * 60)
    print("TOP 100 MOST-CONTESTED ARTICLES")
    print("=" * 60)

    contested_df.show(
        truncate=False
    )

    (
        contested_df.write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(
            f"{output_path}/contested_articles.parquet"
        )
    )

    print("=" * 60)
    print("ALL ANALYSES COMPLETED SUCCESSFULLY")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()