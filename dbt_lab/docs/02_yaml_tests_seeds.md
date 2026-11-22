# Section 2 — YAML: the half of dbt nobody warns you about

In a real dbt repo, roughly **half the lines are YAML**. SQL says *what to build*. YAML says *what
it means, what must be true about it, and how to configure it*. You will write YAML every day.

## 2.1 The four kinds of YAML file

| File | Top-level key | Purpose | Our example |
|------|---------------|---------|-------------|
| `dbt_project.yml` | `name`, `models:`, `vars:` ... | Project identity + **folder-level defaults** | `dbt_project.yml` |
| `profiles.yml` | `<profile_name>:` | Connections | `profiles.yml` |
| **Sources file** | `sources:` | Declares raw tables → enables `source()` | `models/staging/telecom/_telecom__sources.yml` |
| **Properties files** | `models:`, `seeds:`, `snapshots:`, `unit_tests:`, `exposures:` | Docs, tests, per-model config | `_telecom__models.yml`, `_core__models.yml`, `_seeds.yml` |

File *names* for sources/properties don't matter to dbt (it reads every `.yml` under the model
paths). The `_<folder>__models.yml` convention is for humans.

## 2.2 Anatomy of a properties file

Annotated from `models/marts/core/_core__models.yml`:

```yaml
version: 2                        # always 2. Legacy, but still required

models:                           # a LIST. Each item describes one model by name
  - name: fct_calls               # must match the .sql file name exactly
    description: >                # ">" = folded multi-line string
      Incremental fact of call/SMS/data events. Grain: one row per event_id.
    config:                       # same configs as {{ config() }} in the SQL file
      tags: ["finance"]
    columns:                      # a LIST of columns
      - name: event_id
        description: "..."
        data_tests:               # tests ATTACHED to this column
          - unique                #   built-in generic test, no arguments
          - not_null
      - name: customer_sk
        data_tests:
          - relationships:        #   generic test WITH arguments -> it becomes a dict
              arguments:
                to: ref('dim_customer')
                field: customer_sk
```

YAML gotchas that will bite you:
- **Indentation is the syntax.** Two spaces, never tabs. A model indented wrong is silently ignored.
- `- name:` starts a *list item*. Forget the dash and you get a confusing parse error.
- `data_tests:` is the current key (dbt ≥1.8). Older code says `tests:`. Both work, so expect to see both.
- Test arguments go under `arguments:` (newer dbt). Older code puts them directly under the test
  name (`relationships: {to: ..., field: ...}`). Expect to see both in company repos.

## 2.3 Config precedence: where should a setting go?

The same config (e.g. `materialized`) can be set in three places. **The most specific wins**:

```
{{ config(materialized='incremental') }}   in fct_calls.sql          <- wins
config: {materialized: table}               in _core__models.yml
+materialized: table                        under marts: in dbt_project.yml   <- default
```

Rule of thumb:
- **Folder-wide defaults** (staging = view, marts = table, schema names) → `dbt_project.yml` with `+`.
- **Logic that changes how the SQL works** (incremental, unique_key) → `config()` at the top of the `.sql`,
  so the next person reading the SQL sees it.
- **Docs, tests, contracts, tags** → properties YAML.

The `+` prefix in `dbt_project.yml` means "this is a config". Without it, dbt would think
`materialized` is the name of a sub-folder.

## 2.4 Schemas: why your tables land in `main_staging`

`+schema: staging` does **not** produce a schema called `staging`. It produces
`<target schema>_<custom schema>` = `main_staging`. That way every developer gets
`dbt_alice_staging` and `dbt_bob_staging` and nobody overwrites anyone else's work. Many companies
override the `generate_schema_name` macro so that prod writes to plain `staging`. It's a common
interview question.

## 2.5 Tests: the two families

**Data tests** check the *data* after it's built. A test is a query that selects **bad rows**, and
it passes when it returns **zero rows**.

| Kind | Where | Example in this repo |
|------|-------|----------------------|
| Built-in generic | YAML | `unique`, `not_null`, `accepted_values`, `relationships` |
| Custom generic | `tests/generic/*.sql` with `{% test name(model, column_name) %}`, used from YAML like a built-in | `non_negative` → `tests/generic/non_negative.sql` |
| Singular | Any `.sql` in `tests/` | `tests/assert_one_current_version_per_customer.sql` |
| Package tests | `dbt_utils`, `dbt_expectations` (installed via `packages.yml` + `dbt deps`) | Not used here, but you'll see `dbt_utils.unique_combination_of_columns` everywhere |

**Severity** lets you decide what a failure means:

```yaml
- unique:
    config:
      severity: warn     # or error (default). Also: warn_if / error_if: ">10"
```

We set `warn` on the *raw* `event_id` uniqueness check on purpose. Raw has dupes, we know it, and
we want to *see* how many (`WARN 2` on day 3). The same test on the staging model is `error`,
because after dedupe there must be none. **Choosing where a test is a warning and where it's a
hard stop is an engineering decision.**

**Unit tests** (dbt ≥1.8) check the *logic* with hand-written input rows. See the `unit_tests:`
block in `_core__models.yml`: given two calls and a customer who upgraded at 10:00, assert the
08:00 call is priced BASIC and the 15:00 call is free. Unit tests run *before* the model is built,
so broken logic never reaches prod.

```bash
dbt test --select fct_calls               # data + unit tests for fct_calls
```

```bash
dbt test --select test_type:unit
```

## 2.6 Seeds

`seeds/plan_catalog.csv` + `seeds/_seeds.yml`. `dbt seed` loads the CSV as a table, and you
`ref('plan_catalog')` it like a model. Always set `column_types`, otherwise dbt guesses (and will
happily make your `plan_code` an integer if it looks numeric).

Use seeds for **small lookup tables the data team owns** (plan catalog, region mappings, test
accounts to exclude). Don't use them for real data or anything that changes often.

## 2.7 Docs

Every `description:` becomes part of a website:

```bash
dbt docs generate && dbt docs serve
```

Click `fct_calls` → **Lineage graph**. You'll see the full DAG, including the exposures
(dashboards) downstream. This site is what analysts use to find out what a column means.

## 2.8 Contracts (bonus)

`rpt_daily_usage_by_city` has `contract: {enforced: true}`. dbt checks that the SQL output has
**exactly** those column names and types *before* replacing the table. If someone renames
`usage_revenue`, the build fails instead of the Tableau dashboard silently breaking. Use contracts
on models that other teams depend on.

---

## What AI can / cannot do here

| AI does well | AI gets wrong | Your call as the DE |
|---|---|---|
| Generating YAML skeletons with every column listed (`dbt-codegen` does this too) | Indentation, old vs new syntax (`tests:` vs `data_tests:`, `arguments:`), keys that don't exist | **What should be tested?** AI adds `not_null` to everything. You decide which invariants actually matter: grain uniqueness, referential integrity, reconciliation counts |
| Writing descriptions from column names | Writes descriptions that restate the name (`customer_id: "The customer id"`) | **Warn vs error.** Failing prod at 3 a.m. over a cosmetic column is bad, and so is warning on a broken primary key |
| Writing a custom generic test | Doesn't know which dupes are expected in your sources | **Contracts and exposures.** Which models are public interfaces to other teams? |

## Interview questions

1. What does a dbt test return when it passes?
2. Generic vs singular vs unit tests: when would you use each?
3. You set `+schema: marts` but the table landed in `dbt_alice_marts`. Why? How would you change that?
4. Where would you put `materialized='incremental'`, and why there?
5. When is a seed the wrong tool?
