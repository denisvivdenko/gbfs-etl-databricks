from pyspark import pipelines as dp

from gbfs_etl.transformations.quality import DUPLICATE_KEY_COUNT_COLUMN, with_duplicate_key_count
from gbfs_etl.transformations.station_status.silver.station_vehicle_availability import transform_vehicle_availability

_STAGED_NAME = "_silver_station_vehicle_availability_staged"


@dp.materialized_view(
    name=_STAGED_NAME,
    private=True,
    table_properties={"delta.feature.timestampNtz": "supported"},
)
@dp.expect_or_fail("unique_station_id_effective_from", f"{DUPLICATE_KEY_COUNT_COLUMN} = 1")
def _silver_station_vehicle_availability_staged():
    return with_duplicate_key_count(
        transform_vehicle_availability(spark.read.table("bronze_station_status")),
        ["station_id", "effective_from"],
    )


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
    return spark.read.table(_STAGED_NAME).drop(DUPLICATE_KEY_COUNT_COLUMN)
