from pyspark.sql.functions import (
    window,
    col,
    count,
    countDistinct,
    lower,
    when,
    sum,
    max,
    rank,
    to_date
)
from pyspark.sql.window import Window


def edit_volume_per_hour(df):
    """
    Calculate edit volume per article per hour.
    """

    edit_volume_df = (
        df
        .groupBy(
            col("title"),
            window(col("timestamp"), "1 hour")
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


def revert_rate_per_editor(df):
    """
    Calculate revert rate per editor.
    """

    revert_df = (
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

    return revert_df


def contested_articles(df):
    """
    Find Top 100 Most-Contested Articles.
    """

    article_df = (
        df
        .groupBy(
            window(
                col("timestamp"),
                "24 hours"
            ),
            col("title")
        )
        .agg(
            countDistinct("username").alias("unique_editors")
        )
    )

    ranking_window = (
        Window
        .partitionBy("window")
        .orderBy(
            col("unique_editors").desc()
        )
    )

    contested_df = (
        article_df
        .withColumn(
            "rank",
            rank().over(ranking_window)
        )
        .filter(
            col("rank") <= 100
        )
        .withColumn(
            "date",
            to_date(col("window.start"))
        )
        .select(
            "window",
            "title",
            "unique_editors",
            "rank",
            "date"
        )
    )

    return contested_df