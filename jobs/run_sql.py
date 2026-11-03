"""Run a .sql file statement by statement and print every result.

Usage:  python jobs/run_sql.py sql/bronze/00_peek_sources.sql [--env local]

Why this exists: SQL is the primary language of this project. Python only
  1. starts Spark,
  2. replaces ${KEY} placeholders with values from conf/<env>.env
     (local paths now, s3:// paths on AWS later -- the SQL stays identical),
  3. sends each statement to spark.sql() and prints what came back.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parent.parent


def load_env(env: str) -> dict:
    values = {}
    for line in (ROOT / "conf" / f"{env}.env").read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            values[k.strip()] = v.strip()
    return values


def split_statements(sql: str) -> list[str]:
    # A statement ends with ';' at the end of a line. Drop chunks that are only comments.
    parts = re.split(r";\s*$", sql, flags=re.MULTILINE)
    keep = []
    for p in parts:
        code = "\n".join(l for l in p.splitlines() if not l.strip().startswith("--")).strip()
        if code:
            keep.append(p.strip())
    return keep


def build_local_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("telecom-er-local")
        .master("local[2]")                       # 2 task slots: enough for ~80 rows, keeps the UI readable
        .config("spark.sql.shuffle.partitions", "2")  # default 200 would create 200 tiny tasks per shuffle
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("sql_file")
    ap.add_argument("--env", default="local")
    args = ap.parse_args()

    values = load_env(args.env)
    values["RAW_ROOT"] = str((ROOT / values["RAW_ROOT"]).resolve()) if args.env == "local" else values["RAW_ROOT"]
    text = Path(args.sql_file).read_text()
    for k, v in values.items():
        text = text.replace("${" + k + "}", v)

    spark = build_local_session()
    for i, stmt in enumerate(split_statements(text), 1):
        print(f"\n{'=' * 100}\n-- statement {i}\n{stmt}\n{'-' * 100}")
        df = spark.sql(stmt)
        if df.columns:                      # SELECT / SHOW / DESCRIBE return rows; CREATE VIEW returns none
            df.show(100, truncate=False)
    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
