import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import PokeReturnValue, Variable, dag, task

SENSOR_TIMEOUT = int(Variable.get("sensor_timeout_sec"))
SYSTEM_COLS = ("SRC_FILE_NM", "SRC_ROW_NUM", "BUSINESS_DT", "LOAD_TS")


def expected_files(logical_date):
    """The five files for one business date, built from the Variables."""
    feeds = json.loads(Variable.get("pipeline_config"))["feeds"]
    folder = Path(Variable.get("landing_dir"))
    tag = logical_date.strftime("%Y%m%d")
    return [
        {
            "feed": f,
            "file": f"{f}_{tag}.csv",
            "path": str(folder / f"{f}_{tag}.csv"),
            "business_date": logical_date.strftime("%Y-%m-%d"),
        }
        for f in feeds
    ]

def run_sql_file(sql_file, business_date):
    hook = SnowflakeHook(snowflake_conn_id=Variable.get("snowflake_conn_id").strip())
    sql = (Path(Variable.get("sql_dir").strip()) / sql_file).read_text(encoding="utf-8")
    hook.run(sql, parameters={"business_date": business_date})

@dag(
    dag_id="retail_etl",
    start_date=datetime(2026, 11, 9),
    end_date=datetime(2026, 11, 13),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,  # days run one after another, in order
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["assignment-2.3"],
)
def retail_etl():
    @task.sensor(poke_interval=15, timeout=SENSOR_TIMEOUT, mode="reschedule")
    def wait_for_files(logical_date=None) -> PokeReturnValue:
        missing = [f["file"] for f in expected_files(logical_date) if not Path(f["path"]).exists()]
        if missing:
            print(f"Still waiting for: {missing}")
        return PokeReturnValue(is_done=not missing)

    @task
    def list_files(logical_date=None) -> list[dict]:
        return expected_files(logical_date)

    @task
    def load_bronze(item: dict) -> dict:
        cfg = json.loads(Variable.get("pipeline_config"))
        hook = SnowflakeHook(snowflake_conn_id=Variable.get("snowflake_conn_id").strip())
        schema, stage = cfg["bronze_schema"], cfg["stage"]
        name = f"BRZ_{item['feed'].upper()}"
        table = f"{schema}.{name}"
        day = item["business_date"]

        # data columns = every table column except the system ones
        rows = hook.get_records(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
            parameters=(schema.upper(), name),
        )
        cols = [r[0] for r in rows if r[0] not in SYSTEM_COLS]
        picks = ", ".join(f"${i + 1}" for i in range(len(cols)))

        # 1. upload the file, 2. clear this date, 3. load it again
        hook.run(f"PUT file://{item['path']} @{schema}.{stage} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
        hook.run(f"DELETE FROM {table} WHERE BUSINESS_DT = '{day}'")
        hook.run(
            f"COPY INTO {table} ({', '.join(cols)}, SRC_FILE_NM, SRC_ROW_NUM, BUSINESS_DT) "
            f"FROM (SELECT {picks}, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, TO_DATE('{day}') "
            f"FROM @{schema}.{stage}) FILES = ('{item['file']}.gz') FORCE = TRUE"
        )

        # row count check: rows in the table must equal data lines in the file
        with open(item["path"], encoding="utf-8") as fh:
            expected = sum(1 for _ in fh) - 1
        loaded = hook.get_first(f"SELECT COUNT(*) FROM {table} WHERE BUSINESS_DT = '{day}'")[0]
        if loaded != expected:
            raise ValueError(f"{item['file']}: file has {expected} rows, Bronze has {loaded}")
        return {"table": name, "business_date": day, "rows": loaded}

    def silver(task_id, sql_file):
        @task(task_id=task_id)
        def _run(logical_date=None):
            run_sql_file(sql_file, logical_date.strftime("%Y-%m-%d"))
        return _run()
    
    def gold(name):
        @task(task_id=f"gold_{name}")
        def _run(logical_date=None):
            run_sql_file(f"gold/load_{name}.sql", logical_date.strftime("%Y-%m-%d"))
        return _run()

    @task
    def silver_summary(logical_date=None):
        hook = SnowflakeHook(snowflake_conn_id=Variable.get("snowflake_conn_id").strip())
        p = {"d": logical_date.strftime("%Y-%m-%d")}
        rejects = dict(hook.get_records(
            "SELECT REASON_CD, COUNT(*) FROM silver.REJ_ROW WHERE BUSINESS_DT = %(d)s GROUP BY 1", parameters=p))
        kept = hook.get_first("SELECT COUNT(*) FROM silver.SLV_SALES_LINE WHERE BUSINESS_DT = %(d)s", parameters=p)[0]
        bronze = hook.get_first("SELECT COUNT(*) FROM bronze.BRZ_POS_SALES WHERE BUSINESS_DT = %(d)s", parameters=p)[0]
        print(f"Sales: bronze={bronze}, silver={kept}, rejected={rejects}")
        if bronze != kept + sum(rejects.values()):
            raise ValueError("Sales rows lost between Bronze and Silver")
        return {"silver": kept, "rejected": rejects}

    @task
    def reconcile(logical_date=None):
        hook = SnowflakeHook(snowflake_conn_id=Variable.get("snowflake_conn_id").strip())
        p = {"d": logical_date.strftime("%Y-%m-%d")}
        day_ky = "TO_NUMBER(TO_CHAR(%(d)s::DATE, 'YYYYMMDD'))"
        checks = [
            ("sales rows (silver vs gold)",
             "SELECT COUNT(*) FROM retail_etl.silver.SLV_SALES_LINE WHERE BUSINESS_DT = %(d)s",
             f"SELECT COUNT(*) FROM retail_etl.gold.FCT_SALES WHERE DAY_KY = {day_ky}"),
            ("net sales amount (silver vs gold)",
             "SELECT COALESCE(SUM(SLS_AMT),0) FROM retail_etl.silver.SLV_SALES_LINE WHERE BUSINESS_DT = %(d)s",
             f"SELECT COALESCE(SUM(SLS_AMT),0) FROM retail_etl.gold.FCT_SALES WHERE DAY_KY = {day_ky}"),
            ("inventory positions (silver vs gold)",
             "SELECT COUNT(*) FROM retail_etl.silver.SLV_INVENTORY_POSITION WHERE BUSINESS_DT = %(d)s",
             "SELECT COUNT(*) FROM retail_etl.gold.FCT_INVENTORY WHERE EFF_START_DT <= %(d)s AND EFF_END_DT >= %(d)s"),
            ("inventory on-hand total (silver vs gold)",
             "SELECT COALESCE(SUM(ONHAND_QTY),0) FROM retail_etl.silver.SLV_INVENTORY_POSITION WHERE BUSINESS_DT = %(d)s",
             "SELECT COALESCE(SUM(ONHAND_QTY),0) FROM retail_etl.gold.FCT_INVENTORY WHERE EFF_START_DT <= %(d)s AND EFF_END_DT >= %(d)s"),
            ("items with more than one current row (must be 0)",
             "SELECT 0",
             "SELECT COUNT(*) FROM (SELECT ITEM_ID FROM retail_etl.gold.DIM_ITEM WHERE CURR_FLG GROUP BY 1 HAVING COUNT(*) > 1)"),
            ("stores with more than one current row (must be 0)",
             "SELECT 0",
             "SELECT COUNT(*) FROM (SELECT LOCN_ID FROM retail_etl.gold.DIM_LOCN WHERE CURR_FLG GROUP BY 1 HAVING COUNT(*) > 1)"),
        ]
        failed = []
        for name, src_sql, gold_sql in checks:
            src = hook.get_first(src_sql, parameters=p)[0]
            gold = hook.get_first(gold_sql, parameters=p)[0]
            status = "OK" if src == gold else "FAIL"
            print(f"{status:4} | {name}: source={src} gold={gold}")
            if status == "FAIL":
                failed.append(name)
        if failed:
            raise ValueError(f"Reconciliation failed for {logical_date.date()}: {failed}")

    wait = wait_for_files()
    files = list_files()
    wait >> files
    bronze = load_bronze.expand(item=files)

    s_item = silver("silver_item", "silver/load_slv_item.sql")
    s_loc = silver("silver_location", "silver/load_slv_location.sql")
    s_cust = silver("silver_customer", "silver/load_slv_customer.sql")
    s_inv = silver("silver_inventory", "silver/load_slv_inventory_position.sql")
    s_sales = silver("silver_sales", "silver/load_slv_sales_line.sql")

    bronze >> [s_item, s_loc, s_cust, s_inv]
    s_item >> s_sales  # sales checks use that day's item master
    summary = silver_summary()
    [s_loc, s_cust, s_inv, s_sales] >> summary

    # Gold: calendar (parents first), then the two hierarchies and customer
    g_year, g_month = gold("dim_year"), gold("dim_month")
    g_week, g_day = gold("dim_week"), gold("dim_day")
    wait >> g_year >> [g_month, g_week] >> g_day

    g_catg, g_subcatg, g_item = gold("dim_catg"), gold("dim_subcatg"), gold("dim_item")
    g_regn, g_district, g_locn = gold("dim_regn"), gold("dim_district"), gold("dim_locn")
    g_cust = gold("dim_customer")

    s_item >> g_catg >> g_subcatg >> g_item
    s_loc >> g_regn >> g_district >> g_locn
    s_cust >> g_cust

    dims_done = [g_day, g_item, g_locn, g_cust]

    g_sales, g_inv = gold("fct_sales"), gold("fct_inventory")
    (dims_done + [s_sales]) >> g_sales
    (dims_done + [s_inv]) >> g_inv
    [g_sales, g_inv, summary] >> reconcile()


retail_etl()