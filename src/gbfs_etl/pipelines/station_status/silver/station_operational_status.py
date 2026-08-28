from pyspark import pipelines as dp

from gbfs_etl.transformations.quality import DUPLICATE_KEY_COUNT_COLUMN, with_duplicate_key_count
from gbfs_etl.transformations.station_status.silver.station_operational_status import transform_operational_status

_STAGED_NAME = "_silver_station_operational_status_staged"


@dp.materialized_view(
    name=_STAGED_NAME,
    private=True,
    table_properties={"delta.feature.timestampNtz": "supported"},
)
@dp.expect_or_fail("unique_station_id_effective_from", f"{DUPLICATE_KEY_COUNT_COLUMN} = 1")
def _silver_station_operational_status_staged():
    return with_duplicate_key_count(
        transform_operational_status(spark.read.table("bronze_station_status")),
        ["station_id", "effective_from"],
    )


@dp.materialized_view(
    name="silver_station_operational_status",
    comment="Change log of station installation/maintenance flags; one row per "
    "(station_id, effective_from) where (is_installed, is_renting, is_returning) changed.",
    cluster_by=["station_id", "effective_from"],
    table_properties={"quality": "silver", "delta.feature.timestampNtz": "supported"},
)
@dp.expect_all_or_drop({
    "station_id_not_null": "station_id IS NOT NULL",
    "effective_from_not_null": "effective_from IS NOT NULL",
    "is_installed_not_null": "is_installed IS NOT NULL",
    "is_renting_not_null": "is_renting IS NOT NULL",
    "is_returning_not_null": "is_returning IS NOT NULL",
})
def silver_station_operational_status():
    return spark.read.table(_STAGED_NAME).drop(DUPLICATE_KEY_COUNT_COLUMN)
