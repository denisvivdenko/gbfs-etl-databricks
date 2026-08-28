from pyspark import pipelines as dp

from gbfs_etl.transformations.station_status.silver.station_vehicle_type_availability import (
    transform_vehicle_type_availability,
)


@dp.materialized_view(
    name="silver_station_vehicle_type_availability",
    comment="Change log per (station_id, vehicle_type_id) pair; one row per "
    "(station_id, effective_from, vehicle_type_id) where that pair's count changed.",
    cluster_by=["station_id", "effective_from"],
    table_properties={"quality": "silver", "delta.feature.timestampNtz": "supported"},
)
def silver_station_vehicle_type_availability():
    return transform_vehicle_type_availability(spark.read.table("bronze_station_status"))
