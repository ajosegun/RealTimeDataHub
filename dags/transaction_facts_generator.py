from airflow import DAG
from airflow.operators.empty import EmptyOperator
from kafka_operator import KafkaProducerOperator
from datetime import datetime, timedelta

# DAG configurations
start_date = datetime(year=2024, month=9, day=15)
default_args = {
    "owner": "olusegun_owner",
    "depends_on_past": False,
    "backfill": False,
    "start_date": start_date,
}


with DAG(
    dag_id="transaction_facts_generator",
    default_args=default_args,
    start_date=start_date,
    description="Generate transaction facts data into kafka",
    schedule_interval=timedelta(days=1),
    tags=["facts_data"],
) as dag:
    start_empty_dag = EmptyOperator(task_id="start_empty_dag")
    generate_transaction_facts_data = KafkaProducerOperator(
        task_id="generate_transaction_facts_data",
        kafka_broker=["kafka_broker:9092"],
        kafka_topic="transaction_facts",
        num_records=10000,
    )
    end_empty_dag = EmptyOperator(task_id="end_empty_dag")

    start_empty_dag >> generate_transaction_facts_data >> end_empty_dag
