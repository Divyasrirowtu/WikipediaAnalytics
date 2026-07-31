from pyspark.sql.functions import (
    window,
    col,
    count,
    to_date
)


def edit_volume_per_hour(df):
    """
    Calculate edit volume per article per hour.
    """

    edit_volume_df = (
        df
        .groupBy(
            col("title"),
            window(
                col("timestamp"),
                "1 hour"
            )
        )
        .agg(
            count("*").alias("edit_count")
        )
        .withColumn(
            "date",
            to_date(col("window.start"))
        )
        .select(
            "title",
            "window",
            "edit_count",
            "date"
        )
    )

    return edit_volume_df