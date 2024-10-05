from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import sys, types

m = types.ModuleType("kafka.vendor.six.moves", "Mock module")
setattr(m, "range", range)
sys.modules["kafka.vendor.six.moves"] = m
from kafka import KafkaProducer

# from typing import Any
from datetime import datetime, timedelta
import time
import random
import json


class KafkaProducerOperator(BaseOperator):
    @apply_defaults
    def __init__(self, kafka_broker, kafka_topic, num_records=100, *args, **kwargs):
        self.log.info("srat success")
        super(KafkaProducerOperator, self).__init__(*args, **kwargs)
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.num_records = num_records
        self.log.info("init success")

    def generate_transaction_data(self, row_num):
        customer_ids = [f"C{str(i).zfill(5)}" for i in range(1, self.num_records + 1)]
        account_ids = [f"A{str(i).zfill(5)}" for i in range(1, self.num_records + 1)]
        branch_ids = [f"B{str(i).zfill(5)}" for i in range(1, self.num_records + 1)]
        transaction_types = ["Credit", "Debit", "Transfer", "Withdrawal", "Deposit"]
        currencies = ["USD", "EUR", "GBP", "JPY", "CAD"]

        transaction_id = f"T{str(row_num).zfill(6)}"

        # Generating random past transaction date
        # now = datetime.now()
        # random_days_ago = now - timedelta(days=random.randint(0, 3650))

        # self.log.info("type of random_days_ago", type(random_days_ago))
        # transaction_date = int(random_days_ago.timestamp() * 1000)

        # Get current time as a timestamp
        now = time.time()
        one_year_ago = now - 365 * 24 * 60 * 60

        transaction_date = random.randint(int(one_year_ago), int(now))

        account_id = random.choice(account_ids)
        customer_id = random.choice(customer_ids)
        transaction_type = random.choice(transaction_types)
        currency = random.choice(currencies)
        branch_id = random.choice(branch_ids)
        transaction_amount = round(random.uniform(10.00, 10000.00), 2)
        exchange_rate = round(random.uniform(0.50, 1.50), 2)

        self.log.info("Data generated successfully")

        return {
            "transaction_id": transaction_id,
            "transaction_date": transaction_date,
            "account_id": account_id,
            "customer_id": customer_id,
            "transaction_type": transaction_type,
            "currency": currency,
            "branch_id": branch_id,
            "transaction_amount": transaction_amount,
            "exchange_rate": exchange_rate,
        }

    def execute(self, context):
        producer = KafkaProducer(
            bootstrap_servers=self.kafka_broker,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )

        self.log.info("Kafka producer created")

        for i in range(1, self.num_records + 1):
            transaction = self.generate_transaction_data(i)
            producer.send(self.kafka_topic, value=transaction)
            self.log.info(
                f"Sent transaction {transaction} to Kafka topic {self.kafka_topic}"
            )

        producer.flush()
        # producer.close()
        self.log.info(
            f"Sent {self.num_records} transactions to Kafka topic {self.kafka_topic}"
        )
