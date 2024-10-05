import random
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.dag import DAG
from datetime import datetime, timedelta
import pandas as pd
import requests
import json

start_date = datetime(year=2024, month=9, day=15)
default_args = {"owner": "olusegun_owner", "depends_on_past": False, "backfill": False}

num_rows = 50
output_file = "./account_dim_large_data.csv"

account_ids = []
account_types = []
customer_ids = []
statuses = []
balances = []
opening_dates = []


def generate_account_dim(row_num):
    account_id = f"A{row_num:05d}"
    account_type = random.choice(["SAVINGS", "CHECKING", "CREDIT_CARD"])
    status = random.choice(["ACTIVE", "INACTIVE"])
    customer_id = f"C{random.randint(1, 1000):05d}"
    balance = round(random.uniform(100.00, 10000.00), 2)

    now = datetime.now()
    random_days_ago = now - timedelta(days=random.randint(0, 365))
    opening_date_millis = int(random_days_ago.timestamp() * 1000)

    return account_id, account_type, status, customer_id, balance, opening_date_millis


def upload_to_pinot(account_dim_data):
    headers = {"Content-Type": "application/json"}
    pinot_url = "http://pinot-controller:9000/schemas"

    response = requests.post(
        pinot_url, data=json.dumps(account_dim_data), headers=headers
    )

    if response.status_code == 200:
        print("account_dim_data submitted successfully")
    else:
        print("Failed to submit account_dim_data")
        raise Exception(
            f"Failed to submit schema: {response.status_code} account_dim_data"
        )


def generate_account_dim_data():
    for i in range(num_rows):
        account_id, account_type, status, customer_id, balance, opening_date_millis = (
            generate_account_dim(i)
        )
        account_ids.append(account_id)
        account_types.append(account_type)
        customer_ids.append(customer_id)
        statuses.append(status)
        balances.append(balance)
        opening_dates.append(opening_date_millis)

    df = pd.DataFrame(
        {
            "account_id": account_ids,
            "account_type": account_types,
            "customer_id": customer_ids,
            "status": statuses,
            "balance": balances,
            "opening_date": opening_dates,
        }
    )

    df.to_csv(output_file, index=False)

    print(f"CSV file {output_file} has been created with {num_rows} rows.")


with DAG(
    dag_id="account_dim_generator",
    default_args=default_args,
    description="Generate a large CSV file with account dimension data and upload it to Pinot",
    schedule_interval=timedelta(days=1),
    start_date=start_date,
    tags=["account_dim"],
) as dag:
    start = EmptyOperator(task_id="start_task")
    generate_account_dim_data = PythonOperator(
        task_id="generate_account_dim_data",
        python_callable=generate_account_dim_data,
    )
    end = EmptyOperator(task_id="end_task")

    start >> generate_account_dim_data >> end
