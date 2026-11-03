# Interview notes: telecom entity resolution + Iceberg

Short, speakable talking points. Each one is grounded in a record from this repo,
so you can say "for example…" with a real case. Updated after every module.

---

## M0. The problem and the data

**The one-sentence version**
> "We had three systems (inventory, RF planning, field operations) describing the same
> cell sites with no shared key. In Silver we standardized them, matched records across
> and within sources, gave each physical site a canonical ID with a crosswalk back to every
> source ID, and resolved conflicting attributes with explicit survivorship rules."

**Where do you start when there's no shared key?**
- Profile first. State the **grain** of each source: inventory = one row per site; RF = one
  row per plan *version*; field ops = one row per *work order* (many per site).
- List which evidence each **pair** of sources shares:
  - Inventory ↔ RF: name, coordinates, borough → strong
  - Inventory ↔ Field Ops: name, street address, ZIP → strong
  - RF ↔ Field Ops: only a free-text name → weak, so those links come *through* inventory
    (A=B and B=C ⇒ A=C). Inventory is the bridge because it's the only source with both
    coordinates and an address.
- Look for in-source duplicates and data-quality issues (swapped lat/lon, missing ZIPs)
  before matching, because they break matching rules.

**Example I can describe**
- `NY-BRONX-017` (inventory), `RF-10017`, `WO-55001`, `WO-55002` are one site at
  1250 Grand Concourse. `NY-BX-0017` is the *same site again inside inventory*, under a
  legacy ID.
- `NY-BRONX-018` / `RF-10018` / `WO-55003` are a *different* site 597 m away on the same
  street, same ZIP, similar name. Matching on name alone would merge them: a **false positive**.
- `WO-55028` says only "Grand Concourse, Bronx NY", with no house number. We don't guess;
  it goes to **manual review**.

**Who does what on AWS** (expect this question)
- **S3** stores bytes: data files *and* Iceberg metadata files.
- **Spark on EMR (Serverless)** does the compute.
- **Iceberg** is a table format: a spec plus a library inside Spark. It is not a server.
  It defines metadata files (schema, snapshots, lists of data files).
- **Glue Data Catalog** maps `db.table` → the current Iceberg metadata file.
- **Transactions**: there's no transaction manager. A writer writes new files, then asks
  the catalog to swap the pointer *only if it hasn't changed*. That swap is the commit
  (optimistic concurrency).

**Reading formats (why Bronze CSV columns are strings)**
- CSV has no types → everything is a string unless you infer, which costs an extra pass and can guess wrong.
- JSON → Spark does a schema-inference pass over the data; nested objects become `struct`,
  lists become `array`; there's no timestamp type, so timestamps arrive as strings.
- Parquet → the schema is in the file footer; types (including timestamps) come for free.
