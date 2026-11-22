# dbt Mission — Telecom Usage Mart

A small but **real** dbt project. Raw telecom feeds (CRM customers, cell sites, call detail records)
become a star schema that a dashboard and a data-science team consume. It covers sources, `ref`,
YAML, tests, facts & dimensions, **incremental models** and **SCD Type 2 snapshots**, and every
piece runs on your laptop with DuckDB.

## The mission (easy → hard)

| # | Section | You will understand | Read |
|---|---------|---------------------|------|
| 1 | **Foundations** | Where dbt lives, project anatomy, `source()` vs `ref()`, the DAG, compile vs run | [docs/01_foundations.md](docs/01_foundations.md) |
| 2 | **YAML, tests, seeds, docs** | Every YAML file you will write and what each key does | [docs/02_yaml_tests_seeds.md](docs/02_yaml_tests_seeds.md) |
| 3 | **Dimensional modeling** | Facts vs dims, grain, surrogate keys, materializations, macros | [docs/03_dimensional_modeling.md](docs/03_dimensional_modeling.md) |
| 4 | **Incremental models** | `is_incremental()`, `{{ this }}`, `unique_key`, strategies, late data | [docs/04_incremental_models.md](docs/04_incremental_models.md) |
| 5 | **SCD Type 2 + production** | Snapshots, point-in-time joins, who consumes dbt, CI/orchestration | [docs/05_scd2_snapshots_and_production.md](docs/05_scd2_snapshots_and_production.md) |

Each section ends with **What AI can do / What AI gets wrong / Where you must decide**, plus interview questions.

## Setup

```bash
cd dbt_lab
```

```bash
python3 -m venv .venv && source .venv/bin/activate
```

```bash
pip install -r requirements.txt
```

## Run the three "nights" (do them in order)

```bash
python scripts/load_raw.py --reset
```

```bash
python scripts/load_raw.py --day 1 && dbt build
```

```bash
python scripts/load_raw.py --day 2 && dbt build
```

```bash
python scripts/load_raw.py --day 3 && dbt build
```

Look at the results like an analyst would:

```bash
python scripts/query.py "select * from main_marts.dim_customer order by customer_id, valid_from"
```

```bash
python scripts/query.py "select * from main_marts.fct_calls order by event_id"
```

Browse the generated documentation site with the lineage graph:

```bash
dbt docs generate && dbt docs serve
```

## Project map

```
dbt_lab/
├── dbt_project.yml          WHAT the project is + folder-level defaults        (Section 1-2)
├── profiles.yml             HOW to connect (normally lives in ~/.dbt, not git) (Section 1)
├── scripts/load_raw.py      plays Fivetran/Glue: lands raw tables. dbt never loads data.
├── seeds/                   small CSVs owned by the team (plan_catalog)         (Section 2)
├── snapshots/               SCD2 history of the CRM customers table             (Section 5)
├── macros/                  reusable Jinja -> SQL functions                     (Section 3)
├── models/
│   ├── staging/telecom/     1 model per source table: rename/cast/clean/dedupe  (Section 1)
│   │   ├── _telecom__sources.yml   declares raw tables -> source()
│   │   └── _telecom__models.yml    docs + tests for staging models
│   └── marts/
│       ├── core/            facts & dimensions (star schema)                    (Section 3-5)
│       │   └── fct_calls.sql        the INCREMENTAL fact                        (Section 4)
│       └── reporting/       dashboard-shaped aggregates + exposures             (Section 5)
├── tests/                   singular tests + tests/generic/ custom generic tests
└── analyses/                compiled-but-not-run SQL for ad-hoc questions
```

The DAG dbt builds:

```
raw.customers ──► snap_customers ──► stg_telecom__customers_history ──► dim_customer ─┐
raw.call_events ──────────────────► stg_telecom__call_events ─────────────────────────┼─► fct_calls ──► rpt_daily_usage_by_city ──► Tableau dashboard
plan_catalog.csv ─────────────────────────────────────────────────────► dim_plan ─────┘        ▲                                   (exposure)
raw.cell_sites ───────────────────► stg_telecom__cell_sites ──► dim_cell_site                   dim_date
```
