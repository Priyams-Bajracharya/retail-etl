# Architecture Notes: Assignment 2.3

Retail ETL pipeline. These notes accompany the architecture diagram (`architecture/`) and describe what was built in 2.3, which replaces the Northline design from 2.2.

Flow: **Landing files → Bronze → Silver → Gold → Reconcile**, orchestrated by one Airflow DAG (`retail_etl`) on Snowflake (database `retail_etl`, schemas `bronze`, `silver`, `gold`).

---

## 1. Layer contracts

### Landing
Five CSV files per day in `data/landing`: `item_master`, `store_master`, `customer_master`, `pos_sales`, `inventory_snapshot`. The DAG reads only this folder, never the answer files in `data/expected`.

### Bronze: raw, as landed
**Holds:** five tables (`BRZ_ITEM_MASTER`, `BRZ_STORE_MASTER`, `BRZ_CUSTOMER_MASTER`, `BRZ_POS_SALES`, `BRZ_INVENTORY_SNAPSHOT`). All data columns are VARCHAR, plus `SRC_FILE_NM`, `SRC_ROW_NUM`, `BUSINESS_DT`, `LOAD_TS`.

**Guarantees**
- Values are stored as landed, with no casting or cleaning.
- Each row can be traced to its file and row number.
- Each file's Bronze row count equals the file's row count (checked in the same task).
- Loading a date deletes that `BUSINESS_DT` first, so a rerun gives the same result.

**Does not guarantee:** valid types, keys or amounts, or the absence of duplicates.

### Silver: clean and conformed
**Holds:** `SLV_ITEM`, `SLV_LOCATION`, `SLV_CUSTOMER`, `SLV_SALES_LINE`, `SLV_INVENTORY_POSITION`, and the quarantine table `REJ_ROW`.

**Guarantees**
- Typed columns.
- Every Bronze sales row ends up either in `SLV_SALES_LINE` or in `REJ_ROW` with a reason code. Nothing is dropped silently.
- Each table has `BUSINESS_DT` and is reloaded by delete-and-insert for that date.
- A blank `customer_id` is an anonymous sale, not a defect.

**Does not guarantee:** that Gold keys exist yet (checked in Gold and in reconcile).

### Gold: the 2.1 model
**Holds:** 14 tables. Dimensions: `DIM_CATG`, `DIM_SUBCATG`, `DIM_ITEM`, `DIM_REGN`, `DIM_DISTRICT`, `DIM_LOCN`, `DIM_YEAR`, `DIM_MONTH`, `DIM_WEEK`, `DIM_DAY`, `DIM_CUSTOMER`. Facts: `FCT_SALES`, `FCT_INVENTORY`, and `FCT_PRICE` (created, unused).

**Guarantees**
- `DIM_ITEM`: SCD2 on `BRAND_NM` and `SUBCATG_KY`, SCD1 on `ITEM_NM`.
- `DIM_LOCN`: SCD2 on `DISTRICT_KY`, SCD1 on `LOCN_NM`.
- SCD2 versions use `CURR_FLG`, `EFF_START_DT`, `EFF_END_DT` (open row ends 9999-12-31) and do not overlap.
- `FCT_SALES` links each sale to the item and store version valid on the sale date. Anonymous sales use customer key -1.
- `FCT_INVENTORY` rows are closed with `EFF_END_DT` and never deleted.
- Dimension loads run parents before children.

**Does not guarantee**
- `FCT_PRICE` and `INV_VALUE_AMT` are not loaded (no source).
- `COUNTRY_NM` has no source.

---

## 2. Quality gates

| Gate | Where | Rule | Action |
|---|---|---|---|
| 1 | `wait_for_files` | All five files for the date are present before the run continues (timeout from a Variable) | FAIL |
| 2 | `load_bronze` (one mapped task per file) | Bronze row count equals the file row count | FAIL |
| 3 | `silver_sales` | Bad sales rows are moved to `REJ_ROW` with a reason code. The rest continue. | QUARANTINE |
| 4 | `silver_summary` | Bronze sales rows = Silver rows + rejected rows. Prints reject counts by reason. | FAIL |
| 5 | `reconcile` | Silver vs Gold: sales row count and amount, inventory positions and on-hand total. No item or store has two current rows. | FAIL (the DAG run fails) |

**Reject reason codes** (assigned in this priority order): `NULL_ITEM_ID`, `BAD_VALUE`, `NEGATIVE_QTY`, `ITEM_NOT_IN_MASTER`, `DUPLICATE_LINE` (keeps the lowest `SRC_ROW_NUM`).

**Evidence (Day 3, 2026-11-11):** 5 rejects (NULL_ITEM_ID 2, NEGATIVE_QTY 1, ITEM_NOT_IN_MASTER 1, DUPLICATE_LINE 1). 465 clean rows, net sales 42,795.92. The run does not crash.

*(assumption)* Checking daily net sales against the expected totals was a manual proof step. It is not part of the DAG, because the pipeline must not read `data/expected`.

---

## 3. Config and credentials

- Credentials live only in the Airflow Connection `snowflake_retail`.
- Paths, feed list, schema names and timeouts live in Airflow Variables (`landing_dir`, `snowflake_conn_id`, `sensor_timeout_sec`, `pipeline_config`, `sql_dir`).
- One SQL file per table load in `include/sql/silver/` and `include/sql/gold/`. The DAG only runs them with `%(business_date)s`.

---

## 4. Retention policy

*(assumption: proposals, change them if the instructor sets rules)*

| Layer | Retention | Why |
|---|---|---|
| Landing | Keep all | Source files for any replay |
| Bronze | Keep all | Source of truth for reloads |
| Silver | Can be rebuilt from Bronze | Reloaded by date |
| `REJ_ROW` | Keep until the cause is fixed and the rows reloaded | Rejected rows must stay reviewable |
| Gold | Keep all | Reporting layer, holds SCD2 history |

---

## 5. Reload policy: reprocessing one day

**Scenario:** business date D loaded wrongly.

1. Fix the cause (for example, correct a rule or a file).
2. Run the DAG for D (Backfill, set both dates to D, choose "All Runs").
3. Bronze: delete D, reload from the file, check the row count.
4. Silver: delete D from each table and `REJ_ROW`, reinsert.
5. Gold: `FCT_SALES` is deleted and reloaded by `DAY_KY`. Dimension SCD2 loads are guarded by date. `FCT_INVENTORY` only changes for new or changed positions and is never deleted.
6. `reconcile` runs at the end and fails the DAG if Gold does not tie.

**Idempotency proof:** a rerun of Day 5 left row counts and the sales amount identical (DIM_ITEM 153, DIM_LOCN 13, DIM_CUSTOMER 201, FCT_SALES 2034, FCT_INVENTORY 2695, FCT_SALES amount 189,020.80).

**Limitation:** rerun only the latest date. The SCD1 name update has no date guard, so rerunning an older date could write an older name back (for example item 10000020).
