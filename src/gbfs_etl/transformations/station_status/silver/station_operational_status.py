from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"


def transform_operational_status(bronze_df: DataFrame) -> DataFrame:
    """bronze_df: bronze_station_status shape (one row per poll, nested data.stations).

    Returns one row per (station_id, effective_from) where
    (is_installed, is_renting, is_returning) changed from that station's
    previously observed state. See silver_station_status_schema_design.md.
    """
    stations = (
        bronze_df.select(
            F.to_timestamp_ntz(F.col("last_updated"), F.lit(_TS_FORMAT)).alias("effective_from"),
            F.explode("data.stations").alias("station"),
        )
        .select(
            F.col("station.station_id").alias("station_id"),
            "effective_from",
            F.col("station.is_installed").alias("is_installed"),
            F.col("station.is_renting").alias("is_renting"),
            F.col("station.is_returning").alias("is_returning"),
        )
    )

    window = Window.partitionBy("station_id").orderBy("effective_from")
    prev_is_installed = F.lag("is_installed").over(window)
    prev_is_renting = F.lag("is_renting").over(window)
    prev_is_returning = F.lag("is_returning").over(window)

    return (
        stations.withColumn(
            "_is_change",
            prev_is_installed.isNull()
            | (prev_is_installed != F.col("is_installed"))
            | (prev_is_renting != F.col("is_renting"))
            | (prev_is_returning != F.col("is_returning")),
        )
        .filter(F.col("_is_change"))
        .select("station_id", "effective_from", "is_installed", "is_renting", "is_returning")
    )
