from pyspark import pipelines as dp

from gbfs_etl.transformations.station_status.silver.station_vehicle_availability import transform_vehicle_availability


@dp.materialized_view(
    name="silver_station_vehicle_availability",
    comment="Change log of num_vehicles_available; one row per (station_id, effective_from) "
    "where the count changed.",
    cluster_by=["station_id", "effective_from"],
    table_properties={"quality": "silver", "delta.feature.timestampNtz": "supported"},
)
def silver_station_vehicle_availability():
    return transform_vehicle_availability(spark.read.table("bronze_station_status"))
