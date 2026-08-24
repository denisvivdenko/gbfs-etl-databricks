"""Pull the last N records of a Unity Catalog table into fixtures/ for local
transformation development.

Usage:
    uv run python scripts/sample_bronze_table.py
    uv run python scripts/sample_bronze_table.py --limit 50 --table bronze_station_status
"""

import argparse
import json
import os
import pathlib
import sys

from databricks.connect import DatabricksSession
from pyspark.sql.functions import col, struct, to_json

FIXTURES_DIR = pathlib.Path(__file__).parent.parent / "fixtures"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="test", help="Databricks CLI profile")
    parser.add_argument("--catalog", default="gbfs")
    parser.add_argument("--schema", default="denys_vivdenko_k", help="UC schema (dev schema by default)")
    parser.add_argument("--table", default="bronze_station_status")
    parser.add_argument("--order-by", default="_ingested_at", help="Column used to determine the most recent rows")
    parser.add_argument("--limit", type=int, default=5, help="Number of most recent rows to collect")
    parser.add_argument("--output", default=None, help="Output path, default fixtures/<table>_sample.json")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    os.environ["DATABRICKS_CONFIG_PROFILE"] = args.profile
    os.environ.setdefault("DATABRICKS_SERVERLESS_COMPUTE_ID", "auto")

    spark = DatabricksSession.builder.profile(args.profile).getOrCreate()

    full_table_name = f"{args.catalog}.{args.schema}.{args.table}"
    print(f"Collecting last {args.limit} rows from {full_table_name} ordered by {args.order_by}...", file=sys.stderr)

    df = spark.table(full_table_name).orderBy(col(args.order_by).desc()).limit(args.limit)
    json_rows = df.select(to_json(struct(*df.columns)).alias("_json")).collect()
    rows = [json.loads(r["_json"]) for r in json_rows]

    output_path = pathlib.Path(args.output) if args.output else FIXTURES_DIR / f"{args.table}_sample.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(rows, indent=2))

    print(f"Wrote {len(rows)} rows to {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
