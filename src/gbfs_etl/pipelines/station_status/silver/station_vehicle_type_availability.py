from pyspark import pipelines as dp

from gbfs_etl.transformations.quality import DUPLICATE_KEY_COUNT_COLUMN, with_duplicate_key_count
from gbfs_etl.transformations.station_status.silver.station_vehicle_type_availability import (
    transform_vehicle_type_availability,
)

_STAGED_NAME = "_silver_station_vehicle_type_availability_staged"


@dp.materialized_view(
    name=_STAGED_NAME,
    private=True,
    table_properties={"delta.feature.timestampNtz": "supported"},
)
@dp.expect_or_fail(
    "unique_station_id_effective_from_vehicle_type_id", f"{DUPLICATE_KEY_COUNT_COLUMN} = 1"
)
def _silver_station_vehicle_type_availability_staged():
    return with_duplicate_key_count(
        transform_vehicle_type_availability(spark.read.table("bronze_station_status")),
        ["station_id", "effective_from", "vehicle_type_id"],
    )


@dp.materialized_view(
    name="silver_station_vehicle_type_availability",
    comment="Change log per (station_id, vehicle_type_id) pair; one row per "
    "(station_id, effective_from, vehicle_type_id) where that pair's count changed.",
    cluster_by=["station_id", "effective_from"],
    table_properties={"quality": "silver", "delta.feature.timestampNtz": "supported"},
)
@dp.expect_all_or_drop({
    "station_id_not_null": "station_id IS NOT NULL",
    "effective_from_not_null": "effective_from IS NOT NULL",
    "vehicle_type_id_not_null": "vehicle_type_id IS NOT NULL",
    "count_not_null": "`count` IS NOT NULL",
    "count_non_negative": "`count` >= 0",
})
def silver_station_vehicle_type_availability():
    return spark.read.table(_STAGED_NAME).drop(DUPLICATE_KEY_COUNT_COLUMN)
