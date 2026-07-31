import os

from analysis import edit_volume_per_hour, revert_rate_per_editor
from dotenv import load_dotenv

from spark_session import create_spark_session
from ingestion import read_wikipedia_xml
from transform import transform_wikipedia_data
from analysis import edit_volume_per_hour


def main():

    load_dotenv(".env.example")

    spark = create_spark_session()

    input_path = os.getenv("INPUT_PATH")
    output_path = os.getenv("OUTPUT_PATH")

    raw_df = read_wikipedia_xml(
        spark,
        input_path
    )

    transformed_df = transform_wikipedia_data(raw_df)

    edit_volume_df = edit_volume_per_hour(
        revert_rate_df = revert_rate_per_editor(
    transformed_df
)

print("=" * 60)
print("REVERT RATE PER EDITOR")
print("=" * 60)

revert_rate_df.show(truncate=False)

(
    revert_rate_df.write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(
        f"{output_path}/revert_rate.parquet"
    )
)
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

    print("\nEdit volume analysis completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()