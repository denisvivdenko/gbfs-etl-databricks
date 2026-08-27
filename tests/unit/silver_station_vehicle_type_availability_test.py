"""Tests for the silver_station_vehicle_type_availability transform:
bronze_station_status (nested, one row per poll) -> one row per (station_id,
effective_from, vehicle_type_id) where that pair's count changed. Change
detection is per (station_id, vehicle_type_id) pair, independent of the rest
of the station's mix - a type dropping out of vehicle_types_available is a
change to count = 0 for that pair, but only if the station has reported that
type before. See silver_station_status_schema_design.md for the design this
verifies.

Once approved, the logic lives in
src/pipelines/gbfs_etl/transformations/silver_station_vehicle_type_availability.py.
"""

from datetime import datetime

from gbfs_etl.transformations.silver.station_vehicle_type_availability import (
    transform_vehicle_type_availability,
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


def _station(station_id, last_reported, vehicle_types_available):
    return {
        "is_installed": True,
        "is_renting": True,
        "is_returning": True,
        "last_reported": last_reported,
        "num_vehicles_available": sum(vt["count"] for vt in vehicle_types_available),
        "station_id": station_id,
        "vehicle_types_available": vehicle_types_available,
    }


def _vt(vehicle_type_id, count):
    return {"vehicle_type_id": vehicle_type_id, "count": count}


def _rows_as_set(rows):
    return {(r["station_id"], r["effective_from"], r["vehicle_type_id"], r["count"]) for r in rows}


def test_change_detection_is_independent_per_vehicle_type(spark, load_schema):
    """One type's count changes, another's doesn't, on the same poll. Only the
    changed pair gets a new row - the unchanged pair is not repeated.
    """
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    schema = load_schema("bronze_station_status_schema.json")

    polls = [
        _poll(
            [_station("station-a", "2026-08-10T10:00:00Z", [_vt("type-a", 3), _vt("type-b", 2)])],
            last_updated="2026-08-10T10:00:05Z",
        ),
        _poll(
            [_station("station-a", "2026-08-10T11:00:00Z", [_vt("type-a", 3), _vt("type-b", 5)])],
            last_updated="2026-08-10T11:00:05Z",
        ),
    ]
    bronze_df = spark.createDataFrame(polls, schema=schema)

    result = transform_vehicle_type_availability(bronze_df).collect()

    assert _rows_as_set(result) == {
        ("station-a", _parse_ts("2026-08-10T10:00:00Z"), "type-a", 3),
        ("station-a", _parse_ts("2026-08-10T10:00:00Z"), "type-b", 2),
        ("station-a", _parse_ts("2026-08-10T11:00:00Z"), "type-b", 5),
    }


def test_zero_fills_a_previously_seen_type_when_it_drops_out_of_the_array(spark, load_schema):
    """A station's array going empty is a real change for every type it had
    previously reported - each must get an explicit count = 0 row, not be
    silently dropped.
    """
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    schema = load_schema("bronze_station_status_schema.json")

    polls = [
        _poll(
            [_station("station-b", "2026-08-10T10:00:00Z", [_vt("type-a", 3), _vt("type-b", 2)])],
            last_updated="2026-08-10T10:00:05Z",
        ),
        _poll(
            [_station("station-b", "2026-08-10T11:00:00Z", [])],
            last_updated="2026-08-10T11:00:05Z",
        ),
    ]
    bronze_df = spark.createDataFrame(polls, schema=schema)

    result = transform_vehicle_type_availability(bronze_df).collect()

    assert _rows_as_set(result) == {
        ("station-b", _parse_ts("2026-08-10T10:00:00Z"), "type-a", 3),
        ("station-b", _parse_ts("2026-08-10T10:00:00Z"), "type-b", 2),
        ("station-b", _parse_ts("2026-08-10T11:00:00Z"), "type-a", 0),
        ("station-b", _parse_ts("2026-08-10T11:00:00Z"), "type-b", 0),
    }


def test_never_zero_fills_a_type_the_station_has_not_reported(spark, load_schema):
    """A brand-new vehicle_type_id showing up for a station must only appear
    starting at the poll where it's first observed - it must not be backfilled
    onto earlier polls, and its appearance must not cause an unrelated,
    unchanged type to be re-inserted.
    """
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    schema = load_schema("bronze_station_status_schema.json")

    polls = [
        _poll(
            [_station("station-c", "2026-08-10T10:00:00Z", [_vt("type-a", 3)])],
            last_updated="2026-08-10T10:00:05Z",
        ),
        _poll(
            [_station("station-c", "2026-08-10T11:00:00Z", [_vt("type-a", 3), _vt("type-c", 1)])],
            last_updated="2026-08-10T11:00:05Z",
        ),
    ]
    bronze_df = spark.createDataFrame(polls, schema=schema)

    result = transform_vehicle_type_availability(bronze_df).collect()

    assert _rows_as_set(result) == {
        ("station-c", _parse_ts("2026-08-10T10:00:00Z"), "type-a", 3),
        ("station-c", _parse_ts("2026-08-10T11:00:00Z"), "type-c", 1),
    }


def test_real_fixture_runs_cleanly_and_covers_every_reporting_station(spark, load_fixture, load_schema):
    """Smoke test against whatever the sampled fixture currently contains - intended
    to double as a regression check when the fixture is refreshed from prod (e.g.
    after an incident, re-sample the latest N rows with
    scripts/sample_bronze_table.py and rerun this). Deliberately does not assert
    on specific triples or how many rows a change produces, since that's a
    property of the sample's content and can change over time: only on
    invariants that must hold no matter what the bronze data looks like.

    Coverage is checked against stations that report at least one
    vehicle_types_available pair at some poll - a station whose array is empty
    at every poll has nothing to report and is legitimately absent from this
    table.
    """
    schema = load_schema("bronze_station_status_schema.json")
    bronze_df = load_fixture("bronze_station_status_sample.json", schema=schema)
    reporting_station_ids = {
        row["station_id"]
        for row in bronze_df.selectExpr("explode(data.stations) as station")
        .selectExpr("station.station_id as station_id", "explode(station.vehicle_types_available) as vt")
        .select("station_id")
        .distinct()
        .collect()
    }

    result = transform_vehicle_type_availability(bronze_df).collect()

    assert set(result[0].asDict()) == {"station_id", "effective_from", "vehicle_type_id", "count"}
    # Every station that ever reported a vehicle type must produce at least its
    # initial observed pair, regardless of how many times (if any) it changes
    # after that.
    assert {row["station_id"] for row in result} == reporting_station_ids
    for row in result:
        assert row["station_id"] is not None
        assert row["effective_from"] is not None
        assert row["vehicle_type_id"] is not None
