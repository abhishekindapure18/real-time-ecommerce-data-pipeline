from pyspark.sql.functions import col


def apply_data_quality_checks(df):
    """
    Apply basic data quality rules to e-commerce events.
    """

    valid_df = (
        df
        # Required fields must exist
        .filter(col("event_id").isNotNull())
        .filter(col("user_id").isNotNull())
        .filter(col("product_id").isNotNull())
        .filter(col("event_type").isNotNull())

        # Valid business values
        .filter(col("price") > 0)
        .filter(col("quantity") > 0)
        .filter(
            col("event_type").isin(
                "view",
                "cart",
                "purchase"
            )
        )
    )

    return valid_df