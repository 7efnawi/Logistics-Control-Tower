"""
DAG 2: Daily batch
refresh master data -> load daily facts to the Dedicated pool -> dbt run -> dbt test
Each command is a placeholder; replace when the owners finish their parts.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="daily_batch",
    start_date=datetime(2026, 11, 1),
    schedule="0 2 * * *",   # every day at 02:00
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "person1", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["batch", "daily"],
) as dag:
    refresh_master = BashOperator(
        task_id="refresh_master_data",
        bash_command="echo 'TODO: reload carriers/ports/shipments (batch files)'",
    )
    daily_facts = BashOperator(
        task_id="build_daily_facts",
        bash_command="echo 'TODO (person 3): spark-submit spark/daily_facts.py'",
    )
    load_pool = BashOperator(
        task_id="load_facts_to_dedicated_pool",
        bash_command="echo 'TODO (person 4): COPY INTO / CTAS into Dedicated pool'",
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="echo 'TODO: cd dbt && dbt run'",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="echo 'TODO: cd dbt && dbt test'",
    )

    refresh_master >> daily_facts >> load_pool >> dbt_run >> dbt_test
