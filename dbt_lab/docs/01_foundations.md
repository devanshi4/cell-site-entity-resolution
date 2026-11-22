# Section 1 — Foundations: where dbt lives, `source()`, `ref()`, the DAG

## 1.1 What dbt actually is (one sentence)

**dbt is a compiler and runner for `SELECT` statements.** You write a `SELECT`. dbt wraps it in
`CREATE TABLE/VIEW ... AS`, works out the order to run everything in, runs it *inside your
warehouse*, and tests the result.

It is the **T** in ELT:

```
  E + L  (NOT dbt)                       T  (dbt)                                 consumers (NOT dbt)
┌───────────────────────┐     ┌──────────────────────────────────────┐     ┌──────────────────────────┐
│ Fivetran / Airbyte /  │     │ raw ─► staging ─► marts (facts/dims) │     │ Tableau / Looker / Power │
│ Glue / Kafka / Python │ ──► │   all SQL, all run IN the warehouse  │ ──► │ BI, data scientists,     │
│ lands raw tables      │     │   (Snowflake, BigQuery, Redshift,    │     │ reverse-ETL, ML features │
└───────────────────────┘     │    Databricks, Athena, DuckDB...)    │     └──────────────────────────┘
                              └──────────────────────────────────────┘
```

Things dbt does **not** do: move data between systems, run Python ETL (except Python models on
some platforms), or schedule itself. Scheduling comes from Airflow, dbt Cloud, Dagster or a cron job.
**Interview trap:** "dbt is an ETL tool" is wrong. dbt only transforms data that is already in the warehouse.

In this lab, `scripts/load_raw.py` plays the E+L tool, `dbt build` is the T, and `scripts/query.py`
plays the BI tool.

## 1.2 Where you write dbt

dbt is just **files in a git repo**: `.sql` files and `.yml` files. You write them in VS Code (or the
dbt Cloud IDE), commit them, open a pull request, and CI runs `dbt build`. There is no GUI where
the models live. The repo *is* the pipeline.

Two config files tell dbt about the world:

| File | Answers | In git? |
|------|---------|---------|
| `dbt_project.yml` | *What* is this project? Where are the folders? What are the default materializations/schemas? | Yes |
| `profiles.yml` | *How* do I connect? Which warehouse, which credentials, which schema, how many threads? | **No.** It lives in `~/.dbt/` and secrets come from env vars. (We commit ours only because DuckDB has no password.) |

Open both files now. Every line is commented.

## 1.3 A model = one SELECT in one file

`models/staging/telecom/stg_telecom__cell_sites.sql`:

```sql
select
    site_id,
    trim(site_name)       as site_name,
    trim(city)            as city,
    upper(technology)     as technology,
    latitude,
    longitude
from {{ source('telecom', 'cell_sites') }}
```

- The **file name** is the model name *and* the table/view name: `stg_telecom__cell_sites`.
- There is **no `CREATE`** in the file. dbt writes that for you based on the *materialization*.
- `{{ ... }}` is **Jinja**, a templating language. dbt renders it into plain SQL before sending it to the warehouse.

## 1.4 `source()` vs `ref()`: the whole trick

These two functions are the heart of dbt.

| | `{{ source('telecom', 'cell_sites') }}` | `{{ ref('stg_telecom__cell_sites') }}` |
|---|---|---|
| Points at | A **raw table someone else loaded** | Another **dbt object** (model, seed or snapshot) |
| Declared in | A `sources:` block in YAML (`_telecom__sources.yml`) | Nothing extra. The `.sql`/`.csv` file existing is enough |
| Compiles to | `"telecom"."raw"."cell_sites"` | `"telecom"."main_staging"."stg_telecom__cell_sites"` |
| Builds a DAG edge? | Yes (source → model) | Yes (model → model) |

Why not just type `from raw.cell_sites`? Because of three things:

1. **Dependency order (the DAG).** dbt reads every `ref()`/`source()` and builds a graph, so it knows
   `stg_telecom__call_events` must run before `fct_calls`. You never write the run order yourself.
   Hard-code a table name and dbt cannot see the dependency, so it may run things in the wrong order.
2. **Environments.** `ref()` resolves to *your* schema in dev (`dbt_devanshi.fct_calls`) and
   to the prod schema in prod (`analytics.fct_calls`). The same code runs everywhere without edits.
3. **Lineage.** `dbt docs` draws the graph, and `dbt build --select +fct_calls` means "fct_calls and
   everything upstream of it".

> **The rule:** Only staging models use `source()`. Everything else uses `ref()`. If a mart model
> calls `source()`, it skipped the cleaning layer, and that is a code-review flag.

## 1.5 See the compiled SQL (do this, it removes the magic)

```bash
dbt compile --select stg_telecom__cell_sites
```

```bash
cat target/compiled/telecom_dbt/models/staging/telecom/stg_telecom__cell_sites.sql
```

You'll see `from "telecom"."raw"."cell_sites"`. That is plain SQL, with no Jinja left. Then look
at `target/run/...` to see the `create view ... as` that dbt actually executed.

- `target/compiled/` = your SELECT with Jinja rendered. Paste this into a SQL editor when debugging.
- `target/run/` = the full DDL/DML dbt ran (`CREATE VIEW`, `MERGE`, ...).

## 1.6 Layers (why there are folders)

| Layer | Folder | Rule | Materialization |
|-------|--------|------|-----------------|
| **Staging** | `models/staging/<source>/` | 1:1 with a source table. Rename, cast, trim, dedupe. **No joins, no business logic.** | view |
| **Intermediate** | `models/intermediate/` (not needed here) | Reusable joins/logic that aren't for end users | ephemeral/view |
| **Marts** | `models/marts/` | Facts and dimensions that people query | table / incremental |

Naming convention (from dbt Labs, used in most companies): `stg_<source>__<entity>` (double
underscore), `int_...`, `fct_...`, `dim_...`.

## 1.7 Commands you will use every day

| Command | Does |
|---------|------|
| `dbt debug` | Checks profiles.yml and the connection. Run this first on any new machine |
| `dbt parse` / `dbt compile` | Renders Jinja only and runs nothing. Catches syntax errors |
| `dbt run` | Builds models |
| `dbt test` | Runs tests |
| `dbt seed` / `dbt snapshot` | Loads CSV seeds / takes SCD2 snapshots |
| **`dbt build`** | **seed + snapshot + run + test in DAG order. If a model's tests fail, its children are skipped.** This is what prod runs |
| `dbt build --select fct_calls` | Just one model |
| `dbt build --select +fct_calls` | It plus everything upstream |
| `dbt build --select fct_calls+` | It plus everything downstream |
| `dbt ls --select +fct_calls` | Lists what a selector matches without running anything |

## 1.8 Run it

```bash
python scripts/load_raw.py --reset && python scripts/load_raw.py --day 1
```

```bash
dbt build
```

Read the log. Notice the **order**: seeds and snapshot first, then staging, then marts, with tests
running right after the model they test. You never wrote that order down; dbt got it from `ref()`.

---

## What AI can / cannot do here

| AI does well | AI gets wrong (you must catch it) | Your call as the DE |
|---|---|---|
| Writing staging boilerplate: renames, casts, `trim`, `lower` | Hard-codes `from raw.customers` instead of `source()`, which breaks lineage and environments | **Which layer does logic belong in?** AI will happily put a join in staging |
| Generating the `sources.yml` skeleton from a table list | Mixes SQL dialects (e.g. `initcap` doesn't exist in DuckDB; `qualify` doesn't exist in Postgres) | **What does the source really do?** Is it append-only or overwrite? Can it have dupes? Only you can find out, by asking the source team or looking at the data |
| Explaining a compile error | Doesn't know your company's naming/schema conventions | **Dedupe rule.** "Latest `_loaded_at` wins" is a *business* decision. Maybe the switch's first record is the truth instead |

## Interview questions you should now be able to answer

1. What is the difference between `ref` and `source`? Why not hard-code table names?
2. What is the difference between `dbt_project.yml` and `profiles.yml`? Why isn't `profiles.yml` in git?
3. What does `dbt build` do that `dbt run` doesn't?
4. Where do you look to see the SQL dbt actually ran?
5. Is dbt an ETL tool? (No. It's the T in ELT and runs inside the warehouse.)
