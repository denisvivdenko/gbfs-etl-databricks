from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"


def transform_vehicle_availability(bronze_df: DataFrame) -> DataFrame:
    """bronze_df: bronze_station_status shape (one row per poll, nested data.stations).

    Returns one row per (station_id, effective_from) where
    num_vehicles_available changed from that station's previously observed
    value. See silver_station_status_schema_design.md.
    """
    stations = (
        bronze_df.selectExpr("explode(data.stations) as station")
        .select(
            F.col("station.station_id").alias("station_id"),
            F.to_timestamp_ntz(F.col("station.last_reported"), F.lit(_TS_FORMAT)).alias("effective_from"),
            F.col("station.num_vehicles_available").alias("num_vehicles_available"),
        )
    )

    window = Window.partitionBy("station_id").orderBy("effective_from")
    prev_num_vehicles_available = F.lag("num_vehicles_available").over(window)

    return (
        stations.withColumn(
            "_is_change",
            prev_num_vehicles_available.isNull()
            | (prev_num_vehicles_available != F.col("num_vehicles_available")),
        )
        .filter(F.col("_is_change"))
        .select("station_id", "effective_from", "num_vehicles_available")
    )
