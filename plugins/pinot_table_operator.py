from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.context import Context
import glob
import json
import requests


class PinotTableSubmitOperator(BaseOperator):
    @apply_defaults
    def __init__(self, folder_path, pinot_url, *args, **kwargs):
        super(PinotTableSubmitOperator, self).__init__(*args, **kwargs)
        self.folder_path = folder_path
        self.pinot_url = pinot_url

    def execute(self, context: Context):
        try:
            table_files = glob.glob(self.folder_path + "/*.json")
            for table_file in table_files:
                with open(table_file, "r") as file:
                    table_data = json.load(file)

                    self.log.info(f"Table data: {table_data}")

                    headers = {"Content-Type": "application/json"}

                    response = requests.post(
                        self.pinot_url, data=json.dumps(table_data), headers=headers
                    )
                    # response.raise_for_status()
                    if response.status_code == 200:
                        self.log.info(f"Table submitted successfully: {table_file}")
                    else:
                        self.log.error(f"Failed to submit table: {table_file}")
                        raise Exception(f"Failed to submit table: {table_file}")

        except Exception as e:
            self.log.error(f"Error submitting table to Pinot: {str(e)}")
