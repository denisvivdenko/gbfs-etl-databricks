from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"


def transform_vehicle_type_availability(bronze_df: DataFrame) -> DataFrame:
    """bronze_df: bronze_station_status shape (one row per poll, nested data.stations).

    Returns one row per (station_id, effective_from, vehicle_type_id) where
    that pair's count changed from its previously observed value. A
    vehicle_type_id a station has previously reported that drops out of
    vehicle_types_available is treated as count = 0. A vehicle_type_id the
    station has never reported is never zero-filled. See
    silver_station_status_schema_design.md.
    """
    stations = bronze_df.select(
        F.to_timestamp_ntz(F.col("last_updated"), F.lit(_TS_FORMAT)).alias("effective_from"),
        F.explode("data.stations").alias("station"),
    ).select(
        F.col("station.station_id").alias("station_id"),
        "effective_from",
        F.col("station.vehicle_types_available").alias("vehicle_types_available"),
    )

    polls = stations.select("station_id", "effective_from").distinct()

    reported = (
        stations.select("station_id", "effective_from", F.explode("vehicle_types_available").alias("vt"))
        .select("station_id", "effective_from", F.col("vt.vehicle_type_id").alias("vehicle_type_id"), F.col("vt.count").alias("count"))
    )

    first_seen = reported.groupBy("station_id", "vehicle_type_id").agg(
        F.min("effective_from").alias("first_seen")
    )

    expected = polls.join(first_seen, on="station_id").filter(F.col("first_seen") <= F.col("effective_from"))

    observed = expected.join(
        reported, on=["station_id", "effective_from", "vehicle_type_id"], how="left"
    ).select(
        "station_id",
        "effective_from",
        "vehicle_type_id",
        F.coalesce(F.col("count"), F.lit(0)).alias("count"),
    )

    window = Window.partitionBy("station_id", "vehicle_type_id").orderBy("effective_from")
    prev_count = F.lag("count").over(window)

    result = (
        observed.withColumn("_is_change", prev_count.isNull() | (prev_count != F.col("count")))
        .filter(F.col("_is_change"))
        .select("station_id", "effective_from", "vehicle_type_id", "count")
    )
    return result
