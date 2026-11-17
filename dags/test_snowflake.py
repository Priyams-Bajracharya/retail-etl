from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="test_snowflake",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
)
def test_snowflake():
    @task
    def ping():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id="snowflake_retail")
        row = hook.get_first("SELECT CURRENT_DATABASE(), CURRENT_ROLE()")
        print("Connected:", row)

    ping()


test_snowflake()
