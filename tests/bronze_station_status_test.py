"""Local experiments against the bronze_station_status sample fixture, loaded with the
Auto Loader-inferred schema (see scripts/fetch_autoloader_schema.py). Once an experiment
here holds up, port the logic into
src/pipelines/gbfs_etl/transformations/bronze_station_status.py.
"""

from pyspark.sql.functions import col, explode


def test_loads_sample_with_autoloader_schema(load_fixture, load_schema):
    schema = load_schema("bronze_station_status_schema.json")
    df = load_fixture("bronze_station_status_sample.json", schema=schema)

    assert df.count() == 5
    assert set(df.columns) == {"data", "last_updated", "ttl", "version", "_rescued_data"}
