from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

DUPLICATE_KEY_COUNT_COLUMN = "_duplicate_key_count"


def with_duplicate_key_count(df: DataFrame, key_columns: list[str]) -> DataFrame:
    """Add a column counting rows sharing each row's key_columns value.

    Lakeflow expectations can't express PK uniqueness directly: their
    generated WHERE clause rejects window functions
    (WINDOW_FUNCTION_NOT_ALLOWED_IN_CLAUSE). Materializing the count as a
    plain column here lets a downstream `@dp.expect_or_fail` check
    `{DUPLICATE_KEY_COUNT_COLUMN} = 1` without a window function in the
    constraint itself. Must not be called with `.collect()`/other DataFrame
    actions inside a pipeline-decorated function — those run eagerly during
    graph declaration, which Lakeflow Declarative Pipelines doesn't support.
    """
    window = Window.partitionBy(*key_columns)
    return df.withColumn(DUPLICATE_KEY_COUNT_COLUMN, F.count("*").over(window))
