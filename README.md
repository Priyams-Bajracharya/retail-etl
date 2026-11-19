# Retail ETL: Assignment 2.3 (Ycotek Data Engineering, Module 2)

Airflow pipeline that loads five daily retail files into Snowflake through Bronze, Silver and Gold layers,
with SCD2/SCD1 dimensions, quarantine of bad rows and a reconciliation task that fails the DAG on a mismatch.

## Repo layout

| Folder | Content |
|---|---|
| `model/` | 2.1 Gold model (`create_tables.sql`), Bronze (`create_bronze.sql`) and Silver (`create_silver.sql`) DDL |
| `architecture/` | 2.2 architecture documents |
| `dags/` | `retail_etl_dag.py` (dag_id `retail_etl`) |
| `include/sql/` | one SQL file per table load: `silver/` and `gold/` |
| `tests/` | `test_dag.py` (DAG parses, no cycles, retries set, stages exist, no hard-coded credentials) |
| `scripts/` | `generate_data.py` (creates the five daily files, seed 42) |
| `evidence/` | screenshots: backfill grid, reconciliation, rejects, SCD versions, idempotency, tests |
| `data/landing/` | the generated files the DAG reads |

## Pipeline

`wait_for_files` -> `list_files` -> `load_bronze` (one mapped task per file) -> Silver tasks -> Gold dimensions -> Gold facts -> `reconcile`.

- **Bronze:** each file is loaded as text with its file name and row number. Delete-and-reload per business date. A row count check compares the table with the file.
- **Silver:** typed, cleaned data. Bad sales rows go to `REJ_ROW` with a reason code. `silver_summary` checks Bronze sales = Silver + rejected.
- **Gold:** dimensions (calendar, product hierarchy, organisation hierarchy, customer) and two facts. One SQL file per table, parents before children.
- **Reconcile:** compares Gold with Silver (row counts and amounts for sales and inventory) and checks that no item or store has two current rows. Any mismatch raises an error and the DAG run fails.

Business date = the DAG run's logical date. Schedule `@daily`, start 2026-11-09, end 2026-11-13, `max_active_runs=1`, retries 2.

## Data model

Gold follows my 2.1 snowflake model:

- Product: `DIM_CATG` > `DIM_SUBCATG` > `DIM_ITEM` (SCD2 on `BRAND_NM` and `SUBCATG_KY`, SCD1 on `ITEM_NM`).
- Organisation: `DIM_REGN` > `DIM_DISTRICT` > `DIM_LOCN` (SCD2 on `DISTRICT_KY`, SCD1 on `LOCN_NM`).
- Calendar: `DIM_YEAR`, `DIM_MONTH`, `DIM_WEEK`, `DIM_DAY` (seeded for 2026). `DIM_CUSTOMER` has an Unknown row (key -1) for anonymous sales.
- Facts: `FCT_SALES` (grain: one transaction line), `FCT_INVENTORY` (rows are closed with `EFF_END_DT`, never deleted). `FCT_PRICE` exists but is not loaded.
- Naming: the 2.3 slides say `DIM_SUBCLASS`. I kept `DIM_SUBCATG` from my 2.1 model. The file field `subclass` maps to it.
- Mapping (my assumption): file subclass -> `DIM_SUBCATG`, category -> `DIM_CATG`, district -> `DIM_DISTRICT`, region -> `DIM_REGN`.


## How to run

Requirements: Docker Desktop, a Snowflake account. Python is not needed locally.

1. Generate the data (from the repo root, PowerShell):
   `docker run --rm -v "${PWD}:/work" -w /work apache/airflow:3.3.2 python scripts/generate_data.py --out data`
   `data\landing` must contain 25 files.
2. Create a `.env` file in the repo root (it is git-ignored) with two lines:
   `FERNET_KEY=<a key you generate>` and `AIRFLOW_UID=50000`.
   Generate a key with:
   `docker run --rm apache/airflow:3.3.2 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`
3. In Snowflake, run the model files in this order:
   `model/00_create_database.sql`, then `model/create_tables.sql` (Gold),
   `model/create_bronze.sql` and `model/create_silver.sql`.
4. Start Airflow:
   `docker compose up airflow-init` (first time only, creates the database and the airflow/airflow login), then
   `docker compose up -d`. The UI is at http://localhost:8080.
5. In Airflow, create the Connection `snowflake_retail` (type Snowflake, warehouse `COMPUTE_WH`, database `retail_etl`).
   Credentials live only in this Connection.
6. Create these Variables:
   - `landing_dir` = `/opt/airflow/data/landing`
   - `snowflake_conn_id` = `snowflake_retail`
   - `sensor_timeout_sec` = `600`
   - `sql_dir` = `/opt/airflow/include/sql`
   - `pipeline_config` = `{"feeds": ["item_master","store_master","customer_master","pos_sales","inventory_snapshot"], "bronze_schema":"BRONZE","stage":"LANDING_STAGE"}`

## How to backfill

1. Open the DAG `retail_etl`, click **Trigger**, then **Backfill**.
2. Set the dates 2026-11-09 to 2026-11-13 and choose **All Runs**. The days run one after another, in order.
3. To rerun a day that already succeeded, use Backfill with both dates set to that day and **All Runs**
   (the other choices say "No runs matching").

## Idempotency

Bronze and Silver delete and reload by business date. `FCT_SALES` deletes and reloads by `DAY_KY`. Dimensions use MERGE or
close-then-insert, and `FCT_INVENTORY` only closes and opens rows. Running the same date twice leaves all counts and
amounts unchanged (`evidence/06` and `evidence/07`).

## Error handling

Day 3 (2026-11-11) has five planted bad sales rows. They are moved to `REJ_ROW` with a reason code (`NULL_ITEM_ID` 2, `NEGATIVE_QTY` 1,
`ITEM_NOT_IN_MASTER` 1, `DUPLICATE_LINE` 1), counted in `silver_summary`, and the run does not crash (`evidence/04`, `evidence/08`).

## Tests

`tests/test_dag.py` checks the DAG file (parses, no cycles, retries set, expected stages exist, no hard-coded credentials). It does not
check the data. The data is checked by the `reconcile` task. Run the tests:

```
docker compose cp tests/. airflow-worker:/tmp/tests
docker compose exec airflow-worker python -m unittest discover -s /tmp/tests -v
```

## Limitations

- **Database:** I used Snowflake.
- **Load order:** dates must be loaded in order (Day 1 to Day 5). The SCD2 close step has a date guard, but the SCD1 name update does not,
  so rerunning an older date can write an old name back. Only the latest date is safe to rerun.
- **Inventory:** a position that disappears from a later file is not closed. The generated data has no such case.
- **Inventory keys:** an open inventory row keeps the item and store version it was opened with.
- **Empty or unused columns:** `COUNTRY_NM` has no source, `INV_VALUE_AMT` stays empty, `FCT_PRICE` is not loaded.
- **Calendar:** seeded for 2026 only.
- **Answer files:** `data/expected` is only for my own checks and is not mounted into Airflow. The pipeline never reads it.
- **Tests:** they check structure, not data. Reconciliation compares Gold with Silver, so it cannot catch a mistake that is already in Silver.