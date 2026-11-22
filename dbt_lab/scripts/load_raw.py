"""Simulates the UPSTREAM ingestion tool (Fivetran / Airbyte / AWS Glue / Kafka sink).

dbt does NOT load data. Something else lands raw tables in the warehouse; dbt only
transforms what is already there. This script plays that "something else".

    python scripts/load_raw.py --day 1     # first delivery
    python scripts/load_raw.py --day 2     # next night's delivery
    python scripts/load_raw.py --day 3
    python scripts/load_raw.py --reset     # delete the warehouse and start over

Run the days IN ORDER and run `dbt build` after each one. That mimics a nightly
pipeline and is what makes the snapshot (SCD2) and incremental lessons visible.

How each raw table behaves (this matters for dbt design!):
  raw.customers    CRM export, OVERWRITTEN every day with the current state.
                   History is lost upstream -> that is why we need a dbt snapshot (SCD2).
  raw.cell_sites   Small reference table, overwritten daily.
  raw.call_events  CDRs (call detail records), APPEND-only. Each night adds a batch with a
                   new _loaded_at. Big and growing -> that is why fct_calls is incremental.
"""
import argparse
import os

import duckdb

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "telecom.duckdb")

# ---------------------------------------------------------------------------------------
# customers: the CRM's CURRENT state as of each day (customer_id, name, email, city, plan, status, updated_at)
# ---------------------------------------------------------------------------------------
CUSTOMERS_DAY1 = [
    ("C001", "Aarav Shah",  "aarav@example.com",     "Mumbai", "BASIC",    "active", "2026-01-10 09:00:00"),
    ("C002", "Priya Nair",  "priya@example.com",     "Pune",   "BASIC",    "active", "2026-01-12 11:30:00"),
    ("C003", "Rohan Mehta", "rohan@example.com",     "Mumbai", "PREMIUM",  "active", "2026-02-01 08:15:00"),
    ("C004", "Sara Khan",   "sara@example.com",      "Delhi",  "STANDARD", "active", "2026-02-03 14:00:00"),
    # messy source data on purpose: spaces + capitals in the email -> cleaned in staging
    ("C005", "Dev Iyer",    "  DEV.Iyer@Example.com ", "Delhi", "standard", "active", "2026-02-20 10:00:00"),
]


def customers_for(day):
    rows = {r[0]: list(r) for r in CUSTOMERS_DAY1}
    if day >= 2:
        rows["C002"][4] = "PREMIUM"; rows["C002"][6] = "2026-03-02 10:00:00"   # plan upgrade at 10:00
        rows["C004"][3] = "Mumbai";  rows["C004"][6] = "2026-03-02 12:00:00"   # moved city at 12:00
        rows["C006"] = ["C006", "Meera Joshi", "meera@example.com", "Pune", "STANDARD", "active",
                        "2026-03-02 09:00:00"]                                   # brand-new customer
    if day >= 3:
        rows["C002"][5] = "churned"; rows["C002"][6] = "2026-03-03 18:00:00"   # churned
        # TRAP: the CRM changed Rohan's email but did NOT bump updated_at (a real-world bug).
        # The snapshot's `timestamp` strategy will NOT see this change. See docs/05.
        rows["C003"][2] = "rohan.mehta@example.com"
    return [tuple(r) for r in rows.values()]


CELL_SITES = [
    ("S01", "Bandra West Macro", "Mumbai", "5G", 19.0596, 72.8295),
    ("S02", "Andheri East Small", "Mumbai", "4G", 19.1136, 72.8697),
    ("S03", "Hinjewadi Phase 1",  "Pune",   "4G", 18.5913, 73.7389),
    ("S04", "Connaught Place",    "Delhi",  "5G", 28.6315, 77.2167),
]

# ---------------------------------------------------------------------------------------
# call_events: one batch per night. (event_id, customer_id, site_id, event_type, started_at,
#                                     duration_sec, data_mb, _loaded_at)
# ---------------------------------------------------------------------------------------
BATCHES = {
    1: ("2026-03-02 01:00:00", [
        ("E1001", "C001", "S01", "VOICE", "2026-03-01 08:05:00", 300, 0),
        ("E1002", "C002", "S03", "VOICE", "2026-03-01 09:10:00", 600, 0),
        ("E1003", "C002", "S03", "DATA",  "2026-03-01 09:30:00", 0, 250),
        ("E1004", "C003", "S02", "VOICE", "2026-03-01 12:00:00", 120, 0),
        ("E1005", "C004", "S04", "SMS",   "2026-03-01 13:00:00", 0, 0),
        ("E1006", "C005", "S04", "DATA",  "2026-03-01 18:45:00", 0, 1200),
        ("E1007", "C001", "S02", "DATA",  "2026-03-01 21:00:00", 0, 80),
    ]),
    2: ("2026-03-03 01:00:00", [
        ("E1008", "C002", "S03", "VOICE", "2026-03-02 08:00:00", 900, 0),   # BEFORE upgrade -> BASIC price
        ("E1009", "C002", "S03", "VOICE", "2026-03-02 15:00:00", 900, 0),   # AFTER upgrade  -> PREMIUM price
        ("E1010", "C004", "S04", "VOICE", "2026-03-02 09:00:00", 240, 0),   # still in Delhi
        ("E1011", "C004", "S01", "VOICE", "2026-03-02 19:00:00", 240, 0),   # after move to Mumbai
        ("E1012", "C006", "S03", "DATA",  "2026-03-02 11:00:00", 0, 500),
        ("E1013", "C001", "S01", "VOICE", "2026-03-01 23:50:00", 180, 0),   # LATE: happened day 1, arrived day 2
        ("E1002", "C002", "S03", "VOICE", "2026-03-01 09:10:00", 660, 0),   # CORRECTION of E1002 (600 -> 660 s)
    ]),
    3: ("2026-03-04 01:00:00", [
        ("E1014", "C003", "S02", "VOICE", "2026-03-03 10:00:00", 60, 0),
        ("E1015", "C005", "S04", "DATA",  "2026-03-03 14:00:00", 0, 300),
        ("E1016", "C006", "S03", "VOICE", "2026-03-03 16:30:00", 420, 0),
        ("E1016", "C006", "S03", "VOICE", "2026-03-03 16:30:00", 420, 0),   # DUPLICATE: switch sent it twice
        ("E1017", "C001", "S01", "SMS",   "2026-03-03 20:00:00", 0, 0),
    ]),
}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--day", type=int, choices=[1, 2, 3])
    p.add_argument("--reset", action="store_true")
    args = p.parse_args()

    if args.reset:
        for suffix in ("", ".wal"):
            if os.path.exists(DB_PATH + suffix):
                os.remove(DB_PATH + suffix)
        print("Warehouse deleted. Start again with --day 1.")
        return
    if not args.day:
        p.error("pass --day N or --reset")

    con = duckdb.connect(DB_PATH)
    con.execute("create schema if not exists raw")

    # Overwrite-style tables (current state only)
    con.execute("""create or replace table raw.customers (
        customer_id varchar, full_name varchar, email varchar, city varchar,
        plan_code varchar, status varchar, updated_at timestamp)""")
    con.executemany("insert into raw.customers values (?,?,?,?,?,?,?)", customers_for(args.day))

    con.execute("""create or replace table raw.cell_sites (
        site_id varchar, site_name varchar, city varchar, technology varchar,
        latitude double, longitude double)""")
    con.executemany("insert into raw.cell_sites values (?,?,?,?,?,?)", CELL_SITES)

    # Append-only table: rebuild it as "every batch delivered up to this day"
    con.execute("""create or replace table raw.call_events (
        event_id varchar, customer_id varchar, site_id varchar, event_type varchar,
        started_at timestamp, duration_sec integer, data_mb integer, _loaded_at timestamp)""")
    for d in range(1, args.day + 1):
        loaded_at, rows = BATCHES[d]
        con.executemany("insert into raw.call_events values (?,?,?,?,?,?,?,?)",
                        [r + (loaded_at,) for r in rows])

    for t in ("customers", "cell_sites", "call_events"):
        n = con.execute(f"select count(*) from raw.{t}").fetchone()[0]
        print(f"raw.{t:<12} {n:>3} rows")
    print(f"Day {args.day} delivered. Now run:  dbt build")
    con.close()


if __name__ == "__main__":
    main()
