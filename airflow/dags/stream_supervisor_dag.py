"""
DAG 1: Stream supervisor
The Spark stream runs 24/7, so Airflow does NOT run it per batch.
Every 10 minutes this DAG checks that the stream is alive (latest output file is fresh).
If not: restart the job and send an alert.
"""
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils import timezone

# Default fallback values (resolved at task runtime, never at DAG parse time)
DEFAULT_STREAM_OUTPUT_PATH = "/opt/airflow/data/silver/shipment_status"
# For a 24/7 local Spark stream, launch detached so the Airflow worker is not blocked.
# In Azure, replace via Airflow Variable `stream_restart_cmd` with an async Synapse/Livy trigger.
DEFAULT_STREAM_RESTART_CMD = (
    "echo 'TODO: nohup spark-submit /opt/airflow/spark/stream_job.py "
    "> /opt/airflow/logs/stream_job.log 2>&1 < /dev/null &'"
)
DEFAULT_MAX_AGE_MINUTES = "10"


def check_stream_health():
    """Return the next task id: healthy -> 'stream_ok', otherwise -> 'restart_stream'."""
    output_path = Variable.get("stream_output_path", default_var=DEFAULT_STREAM_OUTPUT_PATH)
    max_age_minutes = int(Variable.get("stream_max_age_minutes", default_var=DEFAULT_MAX_AGE_MINUTES))

    p = Path(output_path)
    if not p.exists():
        return "restart_stream"
    newest = max((f.stat().st_mtime for f in p.rglob("*") if f.is_file()), default=0)
    if newest == 0:
        return "restart_stream"
    age_min = (time.time() - newest) / 60
    print(f"newest output is {age_min:.1f} minutes old (max allowed: {max_age_minutes}m)")
    return "stream_ok" if age_min <= max_age_minutes else "restart_stream"


def send_alert(**context):
    # TODO: replace with email / Slack / Teams
    print("ALERT: stream was down and has been restarted at", timezone.utcnow().isoformat())


with DAG(
    dag_id="stream_supervisor",
    start_date=datetime(2026, 11, 1),
    schedule=timedelta(minutes=10),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "person1", "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["stream", "supervisor"],
) as dag:
    check = BranchPythonOperator(task_id="check_stream_health", python_callable=check_stream_health)
    ok = EmptyOperator(task_id="stream_ok")
    restart = BashOperator(
        task_id="restart_stream",
        bash_command="{{ var.value.get('stream_restart_cmd', params.default_cmd) }}",
        params={"default_cmd": DEFAULT_STREAM_RESTART_CMD},
        execution_timeout=timedelta(minutes=2),
    )
    alert = PythonOperator(task_id="send_alert", python_callable=send_alert)

    check >> [ok, restart]
    restart >> alert
