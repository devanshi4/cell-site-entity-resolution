# Section 5 — SCD Type 2 with snapshots, point-in-time joins, and dbt in production

## 5.1 The problem

`raw.customers` is a CRM export that is **overwritten every night**. On night 2 Priya (C002) is
PREMIUM, and the raw table has no memory that she was BASIC yesterday. Finance asks:
*"Was her 08:00 call on 2 March billed at the BASIC rate?"* Without history, you can't answer.

**SCD Type 2** = keep one row per *version* with a validity window:

| customer_id | plan_code | status | valid_from | valid_to |
|---|---|---|---|---|
| C002 | BASIC | active | 2026-01-12 11:30 | 2026-03-02 10:00 |
| C002 | PREMIUM | active | 2026-03-02 10:00 | 2026-03-03 18:00 |
| C002 | PREMIUM | churned | 2026-03-03 18:00 | 9999-12-31 |

## 5.2 dbt snapshots do this for you (`snapshots/snap_customers.yml`)

```yaml
snapshots:
  - name: snap_customers
    relation: source('telecom', 'customers')
    config:
      unique_key: customer_id
      strategy: timestamp
      updated_at: updated_at
      dbt_valid_to_current: "cast('9999-12-31 00:00:00' as timestamp)"
```

What dbt does on every `dbt snapshot` / `dbt build`. This is the **code logic** you should be able to explain:

```
First run:   create snapshot table as
               select *, updated_at as dbt_valid_from, <9999-12-31> as dbt_valid_to,
                      md5(customer_id || updated_at) as dbt_scd_id, now() as dbt_updated_at
             from source

Every run after:
  1. Compare source rows with the CURRENT snapshot rows (dbt_valid_to = 9999-12-31), joined on unique_key
  2. CHANGED  = source.updated_at > snapshot.updated_at        (timestamp strategy)
                or any check_col differs                       (check strategy)
  3. MERGE:
       - for changed rows:  UPDATE old version  SET dbt_valid_to = source.updated_at   (close it)
       - for changed rows:  INSERT new version  with dbt_valid_from = source.updated_at (open it)
       - for brand-new keys: INSERT
       - unchanged rows:    nothing
       - keys that disappeared: nothing by default (see hard_deletes)
```

Look at the real SQL after night 2:

```bash
cat target/run/telecom_dbt/snapshots/snap_customers.yml/snap_customers.sql
```

The path depends on the dbt version. If it isn't there, run `find target/run -name "*snap*"`.

## 5.3 The two strategies: a decision you must make

| | `timestamp` | `check` |
|---|---|---|
| Detects change by | `updated_at` increasing | Comparing the values of `check_cols` |
| `valid_from` comes from | The source's `updated_at` = **when it really changed** | Snapshot run time = **when dbt noticed** |
| Cost | Cheap | Compares every column on every row |
| Fails when | **The source changes data without bumping `updated_at`** | Many cols, or noisy columns (e.g. `last_login`) creating fake versions |

**Night 3 trap. Go and check it:** the CRM changed Rohan's (C003) email but didn't bump
`updated_at`. The timestamp strategy **did not record it**:

```bash
python scripts/query.py "select * from main_snapshots.snap_customers where customer_id = 'C003'"
```

There's still one version, with the old email, while raw has the new one. That's silent data loss.
How would you find out? Compare the current snapshot with the source, or use `check` with
`check_cols: [email, city, plan_code, status]`. Whether you can trust `updated_at` is a question
about the *source system*. The only way to answer it is to investigate, and it's your job.

Other configs you'll need to decide on:
- `hard_deletes: ignore | invalidate | new_record`. What should happen when a customer disappears from the CRM?
- `dbt_valid_to_current`. Use 9999-12-31 instead of NULL for open rows. That makes the BETWEEN join simpler (we use it).
- `unique_key` **must really be unique in the source**. If not, the snapshot corrupts itself
  with overlapping versions. Test the source (we do: `source_unique_telecom_customers_customer_id`).

## 5.4 Snapshot rules that matter in real life

1. **Snapshot the raw source, do the cleaning downstream.** History can't be recreated. If your
   cleaning logic has a bug, you can fix a view on top of the snapshot. You can't fix history you
   never captured. (We clean in `stg_telecom__customers_history`.)
2. **Never drop or `--full-refresh` a snapshot table.** It's the only copy of that history.
   Snapshots sit in their own schema (`main_snapshots`) and are often excluded from cleanup jobs.
3. **Snapshots only see what exists when they run.** If you snapshot nightly and a customer changes plan
   twice in one day, you'll only capture the last change. For full fidelity you need CDC from the source
   (Debezium, DMS), and then you build SCD2 from the change log with window functions instead of snapshots.
4. Run snapshots **before** the models that read them. `dbt build` does this automatically through `ref()`.

## 5.5 Using SCD2 correctly: the point-in-time join

From `fct_calls.sql`:

```sql
left join customers c
    on  e.customer_id = c.customer_id
    and e.started_at >= c.valid_from      -- inclusive
    and e.started_at <  c.valid_to        -- exclusive (so a boundary instant matches exactly ONE version)
```

Result after night 3 (`analyses/revenue_by_plan_version.sql`):

| plan_code | valid_from | valid_to | events | revenue |
|---|---|---|---|---|
| BASIC | 01-12 11:30 | 03-02 10:00 | 3 | 38.00 |
| PREMIUM | 03-02 10:00 | 03-03 18:00 | 1 | 0.00 |

The 08:00 call on 2 March was billed BASIC and the 15:00 call PREMIUM. That's correct history.

Tests that protect this (Section 2): `assert_no_overlapping_customer_versions`,
`assert_one_current_version_per_customer`, `not_null` on `fct_calls.customer_sk` (a NULL means
no version matched, for example because a call was made before the customer's first `valid_from`),
and the unit test that pins the BASIC/PREMIUM pricing.

**Incremental + SCD2 trap:** facts are priced when they're loaded. If the CRM later sends a
*back-dated* change (updated_at = last week), the facts already in `fct_calls` keep the old version.
You need a lookback window, a periodic full refresh, or a decision that this is acceptable. AI
won't raise this. You have to.

## 5.6 Who uses the output, and how

```
                         ┌─► Tableau "Network Usage & Revenue"  ← rpt_daily_usage_by_city   (BI analysts, Network Planning, Finance)
dbt build (nightly) ─────┼─► Data scientists' churn model       ← dim_customer + fct_calls  (status/plan history = features)
 writes to warehouse     ├─► Finance reconciliation / billing    ← fct_calls                 (ad-hoc SQL, analyses/)
                         └─► Reverse ETL (Hightouch/Census)     ← dim_customer → back into the CRM / marketing tools
```

- Consumers never see dbt. They see **tables in the warehouse** (`main_marts.*`, `main_reporting.*`)
  and connect with a read-only role.
- `exposures:` in `models/marts/reporting/_reporting__models.yml` declare those consumers to dbt,
  so the lineage graph shows "changing fct_calls affects the Tableau dashboard and the churn model".
- `dbt docs` is the data catalog analysts use to find out what `usage_charge` means.

## 5.7 dbt in a company: the day-to-day loop

```
 you, on a branch ──► dbt build --select state:modified+   (in YOUR dev schema, e.g. dbt_devanshi_*)
        │
        ▼ pull request
 CI (GitHub Actions / dbt Cloud CI) ──► "slim CI": builds only changed models + children, in a throwaway schema,
        │                                deferring to prod for everything unchanged (--defer --state prod-artifacts/)
        ▼ review + merge to main
 Orchestrator (Airflow / dbt Cloud job / Dagster / MWAA on AWS)
        ──► after ingestion finishes: dbt source freshness → dbt build --target prod
        ──► alerts on failure (Slack/PagerDuty)
```

On AWS (the rest of this repo): the **dbt-athena** adapter (Glue catalog, S3, Iceberg tables
with `table_type='iceberg'` and merge) or **dbt-glue** runs exactly the same project. Only `profiles.yml`
and some dialect details change.

## 5.8 Full end-to-end checklist (run it again, from scratch)

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

```bash
python scripts/query.py "select customer_id, plan_code, city, status, valid_from, valid_to, is_current from main_marts.dim_customer order by 1, 5"
```

Experiments to try yourself (this is where you learn):
1. Change `fct_calls` to filter on `started_at` instead of `_loaded_at`, reset, rerun the 3 nights. Which events are missing?
2. Remove the time conditions from the `dim_customer` join in `fct_calls`. Which test fails first?
3. Switch the snapshot to `strategy: check` with `check_cols: all`, reset, rerun. Does the C003 email change appear now? What's its `valid_from`?
4. Remove `unique_key` from `fct_calls`. What happens to E1002 on night 2?
5. Change the BASIC `per_minute_rate` in the seed and run `dbt build`. Which rows change? Then `--full-refresh`. Which rows change now?

---

## What AI can / cannot do here

| AI does well | AI gets wrong | Your call as the DE |
|---|---|---|
| Writing the snapshot YAML/config | Defaults to `timestamp` without asking whether `updated_at` is trustworthy | **Strategy choice**: you have to *investigate* the source system |
| Writing the point-in-time join | Uses `BETWEEN` (inclusive at both ends, so boundary rows match twice) or forgets the time condition entirely | **Snapshot vs CDC**: is nightly granularity enough for the business? |
| Writing CI YAML and Airflow DAG boilerplate | Suggests `--full-refresh` on a snapshot, which destroys history | **Hard deletes, back-dated changes, retention**: policy decisions with legal/finance implications |
| | Doesn't know which dashboards depend on which model | **Who consumes it**: what breaks if you rename a column (exposures, contracts) |

## Interview questions

1. How do dbt snapshots implement SCD2? What columns do they add?
2. timestamp vs check strategy: when does each fail?
3. Why should you snapshot raw sources rather than transformed models?
4. Write the join from a fact to an SCD2 dimension. Why `>=` and `<` instead of `BETWEEN`?
5. What happens if a customer changes plan twice between two snapshot runs?
6. How does your dbt code get from your laptop to production? What is slim CI?
7. Who consumes your marts, and how would you know what breaks if you change one?
