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
import base64
import time

# Get the Airflow task logger
t_log = logging.getLogger("airflow.task")

# S3 variables
_AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_default")
_S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "my-bucket")
_BUCKET_REGION = os.getenv("BUCKET_REGION", "us-east-1")
_INGEST_FOLDER_NAME = os.getenv("INGEST_FOLDER_NAME", "dutchie_inventory")
_DUTCHIE_LOCATION_NAME = os.getenv("DUTCHIE_LOCATION_NAME","dutchie_location")
# Dutchie API variables
_METRC_API = os.getenv("METRC_API","000")
_METRC_GAPI = os.getenv("METRC_GAPI","000")



# Function to create the Authorization header with Base64 encoded API keys
def create_authorization_header(software_api_key, user_api_key):
    # Combine the API keys
    combined_keys = f"{software_api_key}:{user_api_key}"
    
    # Encode the combined keys in Base64
    encoded_keys = base64.b64encode(combined_keys.encode('utf-8')).decode('utf-8')
    
    # Return the Authorization header with "Basic" followed by the Base64 encoded string
    return f"Basic {encoded_keys}"

# Function to make the API call with the Authorization header
def call_metrc_api_package_by_label(api_server, label, license_number, software_api_key, user_api_key):
    # Create the Authorization header
    auth_header = create_authorization_header(software_api_key, user_api_key)
    
    # Construct the URL
    url = f"{api_server}/packages/v2/{label}?licenseNumber={license_number}"
    
    # Set up the headers with the Authorization header
    headers = {
        'Authorization': auth_header,
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers)
        
        # Check if the request was successful
        if response.status_code == 200:
            package_info = response.json()
        else:
            package_info = {'error': f"Error: {response.status_code}, {response.text}"}

    except Exception as e:
        package_info = {'error': str(e)}

    

    # Return the package info
    return package_info


def call_metrc_api_lab_tests_by_package_id(api_server, id, license_number, software_api_key, user_api_key):
    # Create the Authorization header
    auth_header = create_authorization_header(software_api_key, user_api_key)
    
    # Construct the URL
    url = f"{api_server}/packages/v2/{id}?licenseNumber={license_number}"
    
    # Set up the headers with the Authorization header
    headers = {
        'Authorization': auth_header,
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers)
        
        # Check if the request was successful
        if response.status_code == 200:
            package_info = response.json()
        else:
            package_info = {'error': f"Error: {response.status_code}, {response.text}"}

    except Exception as e:
        package_info = {'error': str(e)}

    

    # Return the package info
    return package_info