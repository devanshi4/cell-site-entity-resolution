"""Run ad-hoc SQL against the local warehouse, like an analyst would from a SQL editor or BI tool.

    python scripts/query.py "select * from main_marts.dim_customer order by customer_id, valid_from"
    python scripts/query.py analyses/revenue_by_plan_version.sql     # or a .sql file

Tip: for files in analyses/ that use {{ ref() }}, run `dbt compile` first and point this at
target/compiled/telecom_dbt/analyses/<file>.sql (the compiled SQL has real table names).
"""
import os
import sys

import duckdb

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "telecom.duckdb")

if len(sys.argv) != 2:
    sys.exit(__doc__)
arg = sys.argv[1]
sql = open(arg).read() if arg.endswith(".sql") else arg

con = duckdb.connect(DB_PATH, read_only=True)
con.sql(sql).show(max_rows=100, max_width=250)
