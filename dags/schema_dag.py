import random
from datetime import datetime, timedelta

from airflow.operators.empty import EmptyOperator
from airflow.models.dag import DAG
from pinot_schema_operator import PinotSchemaSubmitOperator


# DAG configurations
start_date = datetime(year=2024, month=9, day=15)
default_args = {"owner": "olusegun_owner", "depends_on_past": False, "backfill": False}


with DAG(
    dag_id="schema_dag",
    default_args=default_args,
    description="A DAG to submit all schema in a folder to Apache Pinot",
    schedule_interval=timedelta(days=1),
    start_date=start_date,
    tags=["schema"],
) as dag:
    start = EmptyOperator(task_id="start_task")

    submit_schema = PinotSchemaSubmitOperator(
        task_id="submit_schema",
        folder_path="/opt/airflow/dags/schemas",
        pinot_url="http://pinot-controller:9000/schemas",
    )

    end = EmptyOperator(task_id="end_task")

    start >> submit_schema >> end
