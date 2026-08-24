"""Fetch the Auto Loader-inferred schema for a bronze table and save it as Spark
StructType JSON in fixtures/, for use with an explicit `.schema(...)` when loading
local fixtures during development.

The bronze table is materialized directly from the `cloudFiles` (Auto Loader) read, so
its column types reflect what Auto Loader inferred. Columns appended afterward by the
pipeline's `.select()` (file/ingestion metadata) are excluded, since they aren't part of
the schema Auto Loader infers from the source files.

Usage:
    uv run python scripts/fetch_autoloader_schema.py
    uv run python scripts/fetch_autoloader_schema.py --table bronze_station_status
"""

import argparse
import json
import os
import pathlib
import sys

from databricks.connect import DatabricksSession
from pyspark.sql.types import StructType

FIXTURES_DIR = pathlib.Path(__file__).parent.parent / "fixtures"

# Columns bronze_station_status.py appends via .select() after the Auto Loader read.
PIPELINE_ADDED_COLUMNS = {"_source_file", "_file_modified_at", "_ingested_at", "_provider"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="test", help="Databricks CLI profile")
    parser.add_argument("--catalog", default="gbfs")
    parser.add_argument("--schema", default="denys_vivdenko_k", help="UC schema (dev schema by default)")
    parser.add_argument("--table", default="bronze_station_status")
    parser.add_argument("--output", default=None, help="Output path, default fixtures/<table>_schema.json")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    os.environ["DATABRICKS_CONFIG_PROFILE"] = args.profile
    os.environ.setdefault("DATABRICKS_SERVERLESS_COMPUTE_ID", "auto")

    spark = DatabricksSession.builder.profile(args.profile).getOrCreate()

    full_table_name = f"{args.catalog}.{args.schema}.{args.table}"
    print(f"Fetching Auto Loader schema from {full_table_name}...", file=sys.stderr)

    table_schema = spark.table(full_table_name).schema
    raw_schema = StructType([f for f in table_schema.fields if f.name not in PIPELINE_ADDED_COLUMNS])

    output_path = pathlib.Path(args.output) if args.output else FIXTURES_DIR / f"{args.table}_schema.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(raw_schema.jsonValue(), indent=2))

    print(f"Wrote schema ({len(raw_schema.fields)} fields) to {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
