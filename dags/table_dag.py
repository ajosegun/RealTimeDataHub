from datetime import datetime, timedelta

from airflow.operators.empty import EmptyOperator
from airflow.models.dag import DAG
from pinot_table_operator import PinotTableSubmitOperator


# DAG configurations
start_date = datetime(year=2024, month=9, day=15)
default_args = {"owner": "olusegun_owner", "depends_on_past": False, "backfill": False}


with DAG(
    dag_id="table_dag",
    default_args=default_args,
    description="A DAG to submit all tables in a folder to Apache Pinot",
    schedule_interval=timedelta(days=1),
    start_date=start_date,
    tags=["schema"],
) as dag:
    start = EmptyOperator(task_id="start_task")

    submit_tables = PinotTableSubmitOperator(
        task_id="submit_tables",
        folder_path="/opt/airflow/dags/tables",
        pinot_url="http://pinot-controller:9000/tables",
    )

    end = EmptyOperator(task_id="end_task")

    start >> submit_tables >> end
