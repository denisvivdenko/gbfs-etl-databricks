"""This file configures pytest, initializes Databricks Connect, and provides fixtures for Spark and loading test data."""

import os, sys, pathlib
from contextlib import contextmanager

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import pytest
import json
import csv
import os


@pytest.fixture()
def spark() -> SparkSession:
    """Provide a SparkSession fixture for tests.

    Minimal example:
        def test_uses_spark(spark):
            df = spark.createDataFrame([(1,)], ["x"])
            assert df.count() == 1
    """
    return DatabricksSession.builder.getOrCreate()


@pytest.fixture()
def load_fixture(spark: SparkSession):
    """Provide a callable to load JSON or CSV from fixtures/ directory.

    Pass `schema` (e.g. from `load_schema`) to load with an explicit, production-accurate
    schema instead of one inferred from the fixture rows themselves.

    Example usage:

        def test_using_fixture(load_fixture):
            data = load_fixture("my_data.json")
            assert data.count() >= 1

        def test_using_fixture_with_schema(load_fixture, load_schema):
            schema = load_schema("bronze_station_status_schema.json")
            data = load_fixture("bronze_station_status_sample.json", schema=schema)
    """

    def _loader(filename: str, schema: StructType | None = None):
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        suffix = path.suffix.lower()
        if suffix == ".json":
            rows = json.loads(path.read_text())
            return spark.createDataFrame(rows, schema=schema)
        if suffix == ".csv":
            with path.open(newline="") as f:
                rows = list(csv.DictReader(f))
            return spark.createDataFrame(rows, schema=schema)
        raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader


@pytest.fixture()
def load_schema():
    """Provide a callable to load a Spark StructType schema saved as JSON from fixtures/.

    Regenerate schema fixtures with scripts/fetch_autoloader_schema.py.

    Example usage:

        def test_using_schema(load_schema):
            schema = load_schema("bronze_station_status_schema.json")
            assert schema["data"].dataType.typeName() == "struct"
    """

    def _loader(filename: str) -> StructType:
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        return StructType.fromJson(json.loads(path.read_text()))

    return _loader


def _enable_fallback_compute():
    """Enable serverless compute if no compute is specified."""
    conf = WorkspaceClient().config
    if conf.serverless_compute_id or conf.cluster_id or os.environ.get("SPARK_REMOTE"):
        return

    url = "https://docs.databricks.com/dev-tools/databricks-connect/cluster-config"
    print("☁️ no compute specified, falling back to serverless compute", file=sys.stderr)
    print(f"  see {url} for manual configuration", file=sys.stdout)

    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"


@contextmanager
def _allow_stderr_output(config: pytest.Config):
    """Temporarily disable pytest output capture."""
    capman = config.pluginmanager.get_plugin("capturemanager")
    if capman:
        with capman.global_and_fixture_disabled():
            yield
    else:
        yield


def pytest_configure(config: pytest.Config):
    """Configure pytest session."""
    with _allow_stderr_output(config):
        _enable_fallback_compute()

        # Initialize Spark session eagerly, so it is available even when
        # SparkSession.builder.getOrCreate() is used. For DB Connect 15+,
        # we validate version compatibility with the remote cluster.
        if hasattr(DatabricksSession.builder, "validateSession"):
            DatabricksSession.builder.validateSession().getOrCreate()
        else:
            DatabricksSession.builder.getOrCreate()
