from pyspark.sql.functions import (
    explode,
    col,
    to_timestamp,
    to_date,
    coalesce,
    lit
)


def transform_wikipedia_data(df):
    """
    Flatten nested Wikipedia XML data.
    """

    transformed_df = (
        df
        .withColumn("revision", explode(col("revision")))
        .select(
            col("title"),

            col("revision.id").alias("revision_id"),

            to_timestamp(
                col("revision.timestamp"),
                "yyyy-MM-dd'T'HH:mm:ssX"
            ).alias("timestamp"),

            coalesce(
                col("revision.contributor.username"),
                lit("unknown_editor")
            ).alias("username"),

            col("revision.comment").alias("comment"),

            col("revision.sha1").alias("sha1")
        )
        .withColumn(
            "date",
            to_date(col("timestamp"))
        )
    )

    return transformed_df