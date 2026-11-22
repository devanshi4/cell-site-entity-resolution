# Telecom Entity Resolution & Iceberg Lab

A small, hands-on project that rebuilds how an enterprise data team takes three
independent cell-site sources with **no shared key**, identifies which records describe
the same physical site, builds one canonical source of truth, and maintains it reliably
with **Spark SQL + Iceberg** (locally first, then **S3 + EMR Serverless + Glue**).

**Start with the learning guide:** open [`docs/learning-guide.html`](docs/learning-guide.html) in a browser.
Concise talking points live in [`docs/interview-notes.md`](docs/interview-notes.md).

**New: dbt mission.** [`dbt_lab/`](dbt_lab/README.md) is a self-contained dbt project (DuckDB, runs locally)
that teaches sources/ref, YAML, star schemas, incremental models and SCD2 snapshots in 5 sections.

## Setup (local)

Uses the same versions as `spark-perf-lab`: Java 17 or 21, Python 3.9–3.13, PySpark 4.0.5.

```bash
python3 -m venv .venv
```

```bash
source .venv/bin/activate
```

```bash
pip install -r requirements.txt
```

## Run

```bash
python data_generation/generate_sources.py          # (re)creates data/raw and data/ground_truth
```

```bash
python jobs/run_sql.py sql/bronze/00_peek_sources.sql
```

## Layout

```
conf/local.env                 paths for local runs (conf/aws.env comes in Module 13)
data/raw/                      the three source files exactly as "delivered"
  network_inventory/*.csv        Source 1: one row per site, all text
  rf_planning/*.json             Source 2: JSON Lines, nested location + antennas
  field_operations/*.parquet     Source 3: one row per work order, typed timestamps
data/ground_truth/             answer key: true_sites.csv, ground_truth.csv (record -> site + lesson)
data_generation/               hand-curated generator (every record teaches something)
jobs/run_sql.py                runs a .sql file statement by statement, substitutes ${KEY}
sql/bronze/                    SQL per layer (silver/, gold/ added as we reach them)
docs/learning-guide.html       the interactive textbook for this project
docs/interview-notes.md        short interview talking points
```

The project is built one module at a time; see the roadmap in the learning guide.
