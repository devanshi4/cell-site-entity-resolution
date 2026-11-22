# Section 4 — Incremental models: the code logic, line by line

## 4.1 The problem

`raw.call_events` grows by millions of rows a day in a real operator. Rebuilding `fct_calls`
from scratch every night (`materialized='table'`) means rescanning *years* of CDRs to add one
day. Incremental = **process only what's new and merge it into what's already there.**

## 4.2 The code (`models/marts/core/fct_calls.sql`)

```sql
{{
    config(
        materialized = 'incremental',
        unique_key = 'event_id',
        incremental_strategy = 'merge',
        on_schema_change = 'append_new_columns'
    )
}}

with events as (
    select * from {{ ref('stg_telecom__call_events') }}

    {% if is_incremental() %}
    where _loaded_at > (select coalesce(max(_loaded_at), cast('1900-01-01' as timestamp)) from {{ this }})
    {% endif %}
)
...
```

Three pieces of dbt magic:

| Piece | What it is |
|-------|------------|
| `is_incremental()` | A macro that returns **true only if** (1) the model is `incremental`, (2) the table already exists, and (3) you did NOT pass `--full-refresh`. So on the **first** run the `WHERE` is not rendered and the full history loads. |
| `{{ this }}` | The model's **own existing table** in the warehouse (`"telecom"."main_marts"."fct_calls"`). That's how the model asks "how far did I get last time?" |
| `unique_key` | Which column(s) identify a row. With `merge`, matching keys are **updated** and new keys **inserted**. Without a `unique_key`, dbt just appends, and re-processing a row creates a duplicate. |

## 4.3 What dbt actually runs. Look for yourself

After day 2:

```bash
cat target/run/telecom_dbt/models/marts/core/fct_calls.sql
```

You'll see roughly:

```sql
-- 1) build the new rows into a temp table, using your SELECT with the WHERE rendered
create temporary table "fct_calls__dbt_tmp..." as ( ...your select... where _loaded_at > (select max(_loaded_at) from main_marts.fct_calls) );

-- 2) merge them in
MERGE INTO "telecom"."main_marts"."fct_calls" AS DBT_INTERNAL_DEST
USING "fct_calls__dbt_tmp..." AS DBT_INTERNAL_SOURCE
ON DBT_INTERNAL_SOURCE.event_id = DBT_INTERNAL_DEST.event_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

Compare that with day 1, when the file contains a plain `create table ... as`.
**Interview gold:** being able to say "on an incremental run dbt builds a temp table from my
filtered SELECT, then MERGEs it on the unique_key" shows you understand it rather than just use it.

## 4.4 The three nights, and what each one proves

| Night | What arrives in raw | What happens in `fct_calls` | Lesson |
|-------|--------------------|-----------------------------|--------|
| 1 | 7 events | Table created with 7 rows (full load, `is_incremental()` = false) | First run = full build |
| 2 | 7 new rows: 5 new events, **E1013 late** (happened Mar 1, arrived Mar 3), **E1002 correction** (600 → 660 s) | 13 rows. E1013 **inserted**, E1002 **updated** to 660 s | Filter on *load time* catches late data. `merge` + `unique_key` handles corrections |
| 3 | 5 rows incl. **E1016 twice** | 17 rows, no dupes | Dedupe in staging, *before* the merge. Most warehouses fail a MERGE when the source has duplicate keys |

Check it:

```bash
python scripts/query.py "select event_id, started_at, duration_sec, _loaded_at from main_marts.fct_calls order by event_id"
```

## 4.5 The most important decision: what do you filter on?

```sql
where _loaded_at > (select max(_loaded_at) from {{ this }})     -- ✅ what we do
where started_at > (select max(started_at) from {{ this }})     -- ❌ what AI usually writes
```

With `started_at`: on night 2 the max `started_at` already in the table is `2026-03-01 21:00`.
E1013 happened at `2026-03-01 23:50`, so it *would* pass. But if it had happened at `20:00` it would
be **silently dropped forever**. The E1002 correction (`started_at` 09:10, which is old) would also
be **missed**, and nothing would error.

Options, from simplest to most robust:

| Approach | How | Trade-off |
|----------|-----|-----------|
| Load-time watermark | `_loaded_at > max(_loaded_at)` | Needs a trustworthy ingestion timestamp. Best when you have one |
| **Lookback window** | `started_at >= max(started_at) - interval '3 days'` + `unique_key` | Re-processes 3 days every run (merge makes it idempotent). Catches events up to 3 days late. **You choose the 3**, based on how late the source really is |
| `insert_overwrite` by partition | Replace whole `event_date` partitions that changed | Great on BigQuery/Spark/Athena-Iceberg. No row-level merge cost |
| `microbatch` (dbt ≥1.9) | `event_time='started_at'`, `batch_size='day'`, `lookback=3` | dbt generates the filter per batch and lets you backfill a date range. Newer, so check your adapter supports it |

## 4.6 Strategies (`incremental_strategy`)

| Strategy | What dbt does | Use when |
|----------|---------------|----------|
| `append` | `INSERT` only | Immutable events you're sure never repeat. Cheapest |
| `merge` | `MERGE ... ON unique_key` | Rows can be updated/corrected. Default on Snowflake/BigQuery/Databricks |
| `delete+insert` | Delete matching keys, then insert | Warehouses where MERGE is slow or missing (older Redshift/Postgres) |
| `insert_overwrite` | Replace whole partitions | Partitioned big tables (BigQuery, Spark, Athena) |
| `microbatch` | Runs in time batches | Very large time-series with backfills |

Not every adapter supports every strategy, so check your adapter's docs.

On `merge`, consider `incremental_predicates` to limit the target scan
(e.g. only compare against the last 7 days of `fct_calls`). Otherwise MERGE scans the whole target.

## 4.7 `--full-refresh` and `on_schema_change`

```bash
dbt build --select fct_calls --full-refresh
```

This drops the table and rebuilds it from all history. You need it when **the logic changed** (a new
pricing rule has to apply to old rows too). An incremental run only applies new logic to new rows,
so old and new rows quietly disagree. That's a classic incremental bug.

`on_schema_change` decides what happens when you add a column to the SELECT:
`ignore` (default: new column silently not added!), `fail`, `append_new_columns`, `sync_all_columns`.
Old rows get NULL in the new column, so you may still need a full refresh or a backfill.

On huge tables a full refresh is expensive (hours, real money), so many teams set
`full_refresh: false` in config to stop someone running it by accident.

## 4.8 Prove the incremental result is right

`tests/assert_fct_calls_matches_staging.sql` checks that every staged event exists in the fact.
Incremental models drift silently, so a reconciliation test like this is how you find out.
A good extra check: `dbt build --full-refresh` in a dev schema and compare counts with prod.

---

## What AI can / cannot do here

| AI does well | AI gets wrong | Your call as the DE |
|---|---|---|
| The `config()` + `{% if is_incremental() %}` boilerplate | **Filters on event time with no lookback**, silently dropping late data | **Watermark column and lookback size.** Requires knowing how late your sources actually are (ask the source team, or measure `_loaded_at - started_at`) |
| Listing the strategies | Forgets `unique_key` → duplicates on reprocessing | **Strategy** for *your* warehouse, partitioning and update pattern |
| | Doesn't dedupe the batch → MERGE fails on duplicate keys | **When to full-refresh** after a logic change, and how to backfill a 2-TB table safely |
| | Changes business logic in an incremental model without saying old rows are now inconsistent | **Is incremental even needed?** At 10k rows a `table` is simpler and always correct |

## Interview questions

1. Walk me through what happens on the first run vs the second run of an incremental model.
2. What does `is_incremental()` check? What is `{{ this }}`?
3. How do you handle late-arriving data? Why is filtering on event time dangerous?
4. What happens without a `unique_key`? With duplicate keys in the new batch?
5. You changed a calculation in an incremental model. What's wrong with the table now, and how do you fix it?
6. merge vs delete+insert vs insert_overwrite: when would you pick each?
