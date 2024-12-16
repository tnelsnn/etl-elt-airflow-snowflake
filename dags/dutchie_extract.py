from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.utils.task_group import TaskGroup
from pendulum import datetime, duration
import pandas as pd
import logging
import os
import requests
import io

# Get the Airflow task logger
t_log = logging.getLogger("airflow.task")

# S3 variables
_AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_default")
_S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "my-bucket")
_BUCKET_REGION = os.getenv("BUCKET_REGION", "us-east-1")
_INGEST_FOLDER_NAME = os.getenv("INGEST_FOLDER_NAME", "dutchie_inventory")
_LOCATION_NAME = os.getenv("LOCATION_NAME","dutchie_location")
# Dutchie API variables
_DUTCHIE_API_KEY = os.getenv("DUTCHIE_API_KEY")
_CONSUMER_KEY = os.getenv("DUTCHIE_CONSUMER_KEY")
_DUTCHIE_BASE_URL = "https://api.pos.dutchie.com"


def get_auth_dutchie(api_key, base_url='https://api.pos.dutchie.com'):
    
    #this function takes an apiKey and creates the AccessBearer Token required to make other calls to dutchie services

    endpoint = '/util/AuthorizationHeader/{apiKey}'

    url = base_url + endpoint.format(apiKey = api_key)

    try:

        response = requests.get(url)

        if response.status_code == 200:
            return response.json()
        else:
            return f'Error: {response.status_code} - {response.text}'
        
    except requests.exceptions.RequestException as e:

        raise Exception(f"An error occured while trying to connect to Dutchie API: {e}")


def get_inventory_dutchie(api_key, consumer_key = None, include_lab_results = True, include_room_quantities = True, base_url = 'https://api.pos.dutchie.com', endpoint = '/inventory'):

    #this function calls a get request on the inventory this is to be run with the authorized key retrieved from get_auth()
    url = f'{base_url}{endpoint}'
    params = {
        'includeLabResults': include_lab_results,
        'includeRoomQuantities': include_room_quantities
    }

    headers = {
        'Authorization': f'{api_key}',
        'ConsumerKey': f'{consumer_key}' #not required
    }
    try:
        response = requests.get(url, headers = headers, params = params)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            return 'Invalid API Key'
        elif response.status_code == 403:
            return 'Account not authorized'
        elif response.status_code == 500:
            return 'Something went wrong'
        else:
            return f'Unexpected status code: {response.status_code}'
    except requests.exceptions.RequestException as e:

        raise Exception(f"An error occured while trying to connect to Dutchie API: {e}")


@dag(
    dag_id="dutchie_inventory_etl",
    dag_display_name="📊 Extract Dutchie inventory and load to S3",
    start_date=datetime(2024, 8, 1),
    schedule_interval="@daily",
    catchup=False,
    max_consecutive_failed_dag_runs=10,
    default_args={"owner": "Data team", "retries": 3, "retry_delay": duration(minutes=1)},
    params={"num_sales": Param(100, description="Number of records to fetch from the API.", type="number")},
    tags=["ETL", "S3", "Dutchie"],
)
def extract_dutchie_inventory():

    # Task: Create S3 Bucket
    create_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket",
        aws_conn_id=_AWS_CONN_ID,
        bucket_name=_S3_BUCKET_NAME,
        region_name=_BUCKET_REGION,
    )

    @task()
    def fetch_inventory():
        """
        Authenticate with Dutchie API and fetch inventory data.
        """
        t_log.info("Authenticating with Dutchie API...")
        auth_response = get_auth_dutchie(_DUTCHIE_API_KEY)

        # Ensure that the auth_response contains the expected token
        auth_token = auth_response

        t_log.info("Authentication successful.")

        t_log.info("Fetching inventory data from Dutchie API...")
        inventory_data = get_inventory_dutchie(
            api_key=auth_token,
            consumer_key=None,
            include_lab_results=True,
            include_room_quantities=True,
        )

        # Check if inventory_data is a valid JSON response (should be a dictionary)
        if isinstance(inventory_data, list):
            products = inventory_data
            t_log.info(f"Fetched {len(products)} inventory records.")
        else:
            t_log.error(f"Unexpected format for inventory data: {inventory_data}")
            return {}

        return inventory_data


    @task()
    def process_and_write_to_s3(inventory_data):
        """
        Process inventory data and write it to S3 as CSV files.
        """
        # Convert inventory to a DataFrame
        df = pd.DataFrame(inventory_data)
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False)

        # Define S3 path
        path_dst = f"s3://{_S3_BUCKET_NAME}/{_INGEST_FOLDER_NAME}/inventory_{_LOCATION_NAME}.csv"

        # Write data to S3
        t_log.info(f"Uploading inventory data to {path_dst}...")
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        s3 = S3Hook(aws_conn_id=_AWS_CONN_ID)
        s3.load_bytes(
            bytes_data=csv_buffer.getvalue(),
            key=path_dst.split(f"s3://{_S3_BUCKET_NAME}/")[1],
            bucket_name=_S3_BUCKET_NAME,
            replace=True,
        )
        t_log.info("Inventory data successfully uploaded to S3.")

    # Task dependencies
    inventory_data = fetch_inventory()
    create_bucket >> inventory_data >> process_and_write_to_s3(inventory_data)

extract_dutchie_inventory()
