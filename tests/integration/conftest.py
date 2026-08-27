"""Fixtures for integration tests that hit a real deployed Databricks bundle target.

Separate from tests/conftest.py, which sets up local/Databricks Connect Spark
sessions these tests don't need.
"""

import json
import os
import pathlib
import subprocess

import pytest

REPO_ROOT = pathlib.Path(__file__).parent.parent.parent


def pytest_addoption(parser):
    parser.addoption(
        "--db-profile",
        default=os.environ.get("DATABRICKS_CONFIG_PROFILE", "test"),
        help="Databricks CLI profile to use",
    )
    parser.addoption(
        "--bundle-target",
        default="dev",
        help="Bundle target to resolve resource ids from",
    )


@pytest.fixture(scope="session")
def profile(request: pytest.FixtureRequest) -> str:
    return request.config.getoption("--db-profile")


@pytest.fixture(scope="session")
def workspace_client(profile: str):
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient(profile=profile)


@pytest.fixture(scope="session")
def bundle_summary(profile: str, request: pytest.FixtureRequest) -> dict:
    target = request.config.getoption("--bundle-target")
    result = subprocess.run(
        ["databricks", "bundle", "summary", "-t", target, "--profile", profile, "--output", "json"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


@pytest.fixture(scope="session")
def pipeline_id(bundle_summary: dict) -> str:
    return bundle_summary["resources"]["pipelines"]["gbfs_etl"]["id"]
