from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.context import Context
import glob
import json
import requests


class PinotSchemaSubmitOperator(BaseOperator):
    @apply_defaults
    def __init__(self, folder_path, pinot_url, *args, **kwargs):
        super(PinotSchemaSubmitOperator, self).__init__(*args, **kwargs)
        self.folder_path = folder_path
        self.pinot_url = pinot_url

    def execute(self, context: Context):
        try:
            schema_files = glob.glob(self.folder_path + "/*.json")
            for schema_file in schema_files:
                with open(schema_file, "r") as file:
                    schema_data = json.load(file)

                    # self.log.info(f"Schema data: {schema_data}")

                    headers = {"Content-Type": "application/json"}

                    response = requests.post(
                        self.pinot_url, data=json.dumps(schema_data), headers=headers
                    )
                    # response.raise_for_status()
                    self.log.info(f"Response: {response}")
                    if response.status_code == 200:
                        self.log.info(f"Schema submitted successfully: {schema_file}")
                    else:
                        self.log.error(f"Failed to submit schema: {schema_file}")
                        raise Exception(
                            f"Failed to submit schema: {response.status_code} {schema_file}"
                        )

        except Exception as e:
            self.log.error(f"Error submitting schema to Pinot: {str(e)}")
