from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def assert_unique(df: DataFrame, key_columns: list[str]) -> DataFrame:
    """Fail fast if df has more than one row per key_columns.

    Lakeflow expectations can't express PK uniqueness: their generated WHERE
    clause rejects window functions (WINDOW_FUNCTION_NOT_ALLOWED_IN_CLAUSE), so
    this is enforced eagerly instead, before the materialized view commits.
    """
    duplicates = df.groupBy(*key_columns).count().filter(F.col("count") > 1)
    offending = duplicates.limit(5).collect()
    if offending:
        raise ValueError(f"Duplicate rows for key {key_columns}: {offending}")
    return df
