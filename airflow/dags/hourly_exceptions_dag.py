"""
DAG 3: Hourly exceptions
Run the exception report (at risk / stuck / silent) and send SLA alerts.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


def send_sla_alerts(**context):
    # TODO: read the report result and alert if counts pass a threshold
    print("SLA check done at", timezone.utcnow().isoformat())


with DAG(
    dag_id="hourly_exceptions",
    start_date=datetime(2026, 11, 1),
    schedule="0 * * * *",   # every hour
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "person1", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["exceptions", "hourly"],
) as dag:
    report = BashOperator(
        task_id="run_exception_report",
        bash_command="echo 'TODO (person 5): run sql/exception_report.sql'",
    )
    alerts = PythonOperator(task_id="send_sla_alerts", python_callable=send_sla_alerts)

    report >> alerts
