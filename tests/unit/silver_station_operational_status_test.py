"""Tests for the silver_station_operational_status transform: bronze_station_status
(nested, one row per poll) -> one row per (station_id, effective_from) where
(is_installed, is_renting, is_returning) changed. See
silver_station_status_schema_design.md for the design this verifies.

Once approved, the logic lives in
src/pipelines/gbfs_etl/transformations/silver_station_operational_status.py.
"""

from datetime import datetime

from gbfs_etl.transformations.silver.station_operational_status import (
    transform_operational_status,
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


def _station(
    station_id,
    last_reported,
    is_installed=True,
    is_renting=True,
    is_returning=True,
    num_vehicles_available=0,
    vehicle_types_available=None,
):
    return {
        "is_installed": is_installed,
        "is_renting": is_renting,
        "is_returning": is_returning,
        "last_reported": last_reported,
        "num_vehicles_available": num_vehicles_available,
        "station_id": station_id,
        "vehicle_types_available": vehicle_types_available or [],
    }


def test_real_fixture_runs_cleanly_and_covers_every_station(spark, load_fixture, load_schema):
    """Smoke test against whatever the sampled fixture currently contains - intended
    to double as a regression check when the fixture is refreshed from prod (e.g.
    after an incident, re-sample the latest N rows with
    scripts/sample_bronze_table.py and rerun this). Deliberately does not assert
    on specific values, since the sample's content (and whether flags actually
    change in it) can change over time: only on invariants that must hold no
    matter what the bronze data looks like.
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

    result = transform_operational_status(bronze_df).collect()

    assert set(result[0].asDict()) == {
        "station_id",
        "effective_from",
        "is_installed",
        "is_renting",
        "is_returning",
    }
    # Every station must produce at least its initial observed state, regardless
    # of how many times (if any) it changes after that.
    assert {row["station_id"] for row in result} == bronze_station_ids
    for row in result:
        assert row["station_id"] is not None
        assert row["effective_from"] is not None


def test_inserts_a_new_row_each_time_flags_actually_change(spark, load_schema):
    """A station whose flags flip and then revert must get a row for every
    distinct state observed in order - reverting to a prior state is still a
    change, not a no-op. A second station with unchanged flags across the same
    polls must collapse to a single row, independently of the first.
    """
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    schema = load_schema("bronze_station_status_schema.json")

    polls = [
        _poll(
            [
                _station("station-changes", "2026-08-10T10:00:00Z", is_renting=True),
                _station("station-stable", "2026-08-10T10:00:00Z"),
            ],
            last_updated="2026-08-10T10:00:05Z",
        ),
        _poll(
            [
                _station("station-changes", "2026-08-10T11:00:00Z", is_renting=False),
                _station("station-stable", "2026-08-10T10:00:00Z"),
            ],
            last_updated="2026-08-10T11:00:05Z",
        ),
        _poll(
            [
                _station("station-changes", "2026-08-10T12:00:00Z", is_renting=True),
                _station("station-stable", "2026-08-10T10:00:00Z"),
            ],
            last_updated="2026-08-10T12:00:05Z",
        ),
    ]
    bronze_df = spark.createDataFrame(polls, schema=schema)

    result = (
        transform_operational_status(bronze_df)
        .orderBy("station_id", "effective_from")
        .collect()
    )

    changes = [r for r in result if r["station_id"] == "station-changes"]
    assert [r["is_renting"] for r in changes] == [True, False, True]
    assert [r["effective_from"] for r in changes] == [
        _parse_ts("2026-08-10T10:00:00Z"),
        _parse_ts("2026-08-10T11:00:00Z"),
        _parse_ts("2026-08-10T12:00:00Z"),
    ]

    stable = [r for r in result if r["station_id"] == "station-stable"]
    assert len(stable) == 1
    assert stable[0]["effective_from"] == _parse_ts("2026-08-10T10:00:00Z")


def test_vehicle_count_changes_do_not_trigger_a_new_operational_status_row(spark, load_schema):
    """Design goal: rare-changing operational flags must not be duplicated just
    because a high-frequency field (num_vehicles_available) changed and bumped
    last_reported. A station with constant flags but a different vehicle count
    (and different last_reported) on every poll must still collapse to one row.
    """
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    schema = load_schema("bronze_station_status_schema.json")

    polls = [
        _poll(
            [_station("station-busy", "2026-08-10T10:00:00Z", num_vehicles_available=0)],
            last_updated="2026-08-10T10:00:05Z",
        ),
        _poll(
            [_station("station-busy", "2026-08-10T11:00:00Z", num_vehicles_available=5)],
            last_updated="2026-08-10T11:00:05Z",
        ),
        _poll(
            [_station("station-busy", "2026-08-10T12:00:00Z", num_vehicles_available=2)],
            last_updated="2026-08-10T12:00:05Z",
        ),
    ]
    bronze_df = spark.createDataFrame(polls, schema=schema)

    result = transform_operational_status(bronze_df).collect()

    assert len(result) == 1
    assert result[0]["effective_from"] == _parse_ts("2026-08-10T10:00:00Z")
