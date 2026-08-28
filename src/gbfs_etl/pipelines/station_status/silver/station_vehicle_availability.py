from pyspark import pipelines as dp

from gbfs_etl.transformations.station_status.silver.station_vehicle_availability import transform_vehicle_availability


@dp.materialized_view(
    name="silver_station_vehicle_availability",
    comment="Change log of num_vehicles_available; one row per (station_id, effective_from) "
    "where the count changed.",
    cluster_by=["station_id", "effective_from"],
    table_properties={"quality": "silver", "delta.feature.timestampNtz": "supported"},
)
@dp.expect_all_or_drop({
    "station_id_not_null": "station_id IS NOT NULL",
    "effective_from_not_null": "effective_from IS NOT NULL",
    "num_vehicles_available_not_null": "num_vehicles_available IS NOT NULL",
    "num_vehicles_available_non_negative": "num_vehicles_available >= 0",
})
def silver_station_vehicle_availability():
    return transform_vehicle_availability(spark.read.table("bronze_station_status"))
