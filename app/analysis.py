from pyspark.sql.functions import (
    window,
    col,
    count,
    to_date
)


def edit_volume_per_hour(df):

    from pyspark.sql.functions import (
    lower,
    when,
    sum,
    max,
    col
)


def revert_rate_per_editor(df):
    """
    Calculate revert rate per editor.
    """

    editor_stats = (
        df
        .withColumn(
            "is_reverted",
            when(
                lower(col("comment")).contains("reverted"),
                1
            ).otherwise(0)
        )
        .groupBy("username")
        .agg(
            count("*").alias("total_edits"),
            sum("is_reverted").alias("reverted_edits"),
            max("timestamp").alias("latest_edit")
        )
        .withColumn(
            "revert_rate",
            col("reverted_edits") / col("total_edits")
        )
        .withColumn(
            "date",
            to_date(col("latest_edit"))
        )
        .select(
            "username",
            "total_edits",
            "reverted_edits",
            "revert_rate",
            "date"
        )
    )

    return editor_stats

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