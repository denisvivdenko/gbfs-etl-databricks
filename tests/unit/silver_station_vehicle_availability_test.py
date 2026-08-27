"""Tests for the silver_station_vehicle_availability transform: bronze_station_status
(nested, one row per poll) -> one row per (station_id, effective_from) where
num_vehicles_available changed. See silver_station_status_schema_design.md for
the design this verifies.

Once approved, the logic lives in
src/pipelines/gbfs_etl/transformations/silver_station_vehicle_availability.py.
"""

from datetime import datetime

from gbfs_etl.transformations.silver.station_vehicle_availability import (
    transform_vehicle_availability,
)


def _parse_ts(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")


def _poll(stations, last_updated="2026-08-21T17:48:23Z"):
    return {
        "data": {"stations": stations},
        "last_updated": last_updated,
        "ttl": 0,
        "version": "3.0",
        "_rescued_data": None,
    }


def _station(station_id, last_reported, num_vehicles_available):
    return {
        "is_installed": True,
        "is_renting": True,
        "is_returning": True,
        "last_reported": last_reported,
        "num_vehicles_available": num_vehicles_available,
        "station_id": station_id,
        "vehicle_types_available": [],
    }


def test_real_fixture_runs_cleanly_and_covers_every_station(spark, load_fixture, load_schema):
    """Smoke test against whatever the sampled fixture currently contains - intended
    to double as a regression check when the fixture is refreshed from prod (e.g.
    after an incident, re-sample the latest N rows with
    scripts/sample_bronze_table.py and rerun this). Deliberately does not assert
    on specific values or on how many times counts change in the sample, since
    that's a property of the sample's content and can change over time: only on
    invariants that must hold no matter what the bronze data looks like.
    """
    schema = load_schema("bronze_station_status_schema.json")
    bronze_df = load_fixture("bronze_station_status_sample.json", schema=schema)
    bronze_station_ids = {
        row["station_id"]
        for row in bronze_df.selectExpr("explode(data.stations) as station")
        .selectExpr("station.station_id as station_id")
        .distinct()
        .collect()
    }

    result = transform_vehicle_availability(bronze_df).collect()

    assert set(result[0].asDict()) == {"station_id", "effective_from", "num_vehicles_available"}
    # Every station must produce at least its initial observed value, regardless
    # of how many times (if any) it changes after that.
    assert {row["station_id"] for row in result} == bronze_station_ids
    for row in result:
        assert row["station_id"] is not None
        assert row["effective_from"] is not None


def test_reinserts_a_row_when_count_returns_to_a_previous_value(spark, load_schema):
    """A naive dedup (e.g. distinct() on (station_id, count)) would collapse a
    3 -> 5 -> 3 sequence down to two rows, silently merging the first and third
    polls. Each poll differs from its immediate predecessor, so all three must
    produce a row.
    """
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    schema = load_schema("bronze_station_status_schema.json")

    polls = [
        _poll([_station("station-a", "2026-08-10T10:00:00Z", 3)], last_updated="2026-08-10T10:00:05Z"),
        _poll([_station("station-a", "2026-08-10T11:00:00Z", 5)], last_updated="2026-08-10T11:00:05Z"),
        _poll([_station("station-a", "2026-08-10T12:00:00Z", 3)], last_updated="2026-08-10T12:00:05Z"),
    ]
    bronze_df = spark.createDataFrame(polls, schema=schema)

    result = (
        transform_vehicle_availability(bronze_df)
        .orderBy("effective_from")
        .collect()
    )

    assert [r["num_vehicles_available"] for r in result] == [3, 5, 3]
    assert [r["effective_from"] for r in result] == [
        _parse_ts("2026-08-10T10:00:00Z"),
        _parse_ts("2026-08-10T11:00:00Z"),
        _parse_ts("2026-08-10T12:00:00Z"),
    ]
