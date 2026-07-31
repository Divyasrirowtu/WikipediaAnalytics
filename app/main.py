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

    print("=" * 70)
    print("WIKIPEDIA ANALYTICS PIPELINE")
    print("=" * 70)

    print("\nReading Wikipedia XML...")

    raw_df = read_wikipedia_xml(
        spark,
        input_path
    )

    print("XML Loaded Successfully")

    print("\nTransforming Data...")

    transformed_df = transform_wikipedia_data(raw_df)

    print("Transformation Completed")

    print("\nTotal Records")

    print(transformed_df.count())

    transformed_df.show(10, truncate=False)

    # ---------------------------------------
    # Analysis 1
    # ---------------------------------------

    print("\nRunning Analysis 1...")

    edit_volume_df = edit_volume_per_hour(transformed_df)

    edit_volume_df.show(truncate=False)

    (
        edit_volume_df.write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(
            f"{output_path}/edit_volume.parquet"
        )
    )

    print("Edit Volume Written")

    # ---------------------------------------
    # Analysis 2
    # ---------------------------------------

    print("\nRunning Analysis 2...")

    revert_rate_df = revert_rate_per_editor(
        transformed_df
    )

    revert_rate_df.show(truncate=False)

    (
        revert_rate_df.write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(
            f"{output_path}/revert_rate.parquet"
        )
    )

    print("Revert Rate Written")

    # ---------------------------------------
    # Analysis 3
    # ---------------------------------------

    print("\nRunning Analysis 3...")

    contested_df = contested_articles(
        transformed_df
    )

    contested_df.show(truncate=False)

    (
        contested_df.write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(
            f"{output_path}/contested_articles.parquet"
        )
    )

    print("Contested Articles Written")

    print("\n")
    print("=" * 70)
    print("PIPELINE EXECUTED SUCCESSFULLY")
    print("=" * 70)

    print("\nOutput Location")

    print(output_path)

    spark.stop()


if __name__ == "__main__":
    main()