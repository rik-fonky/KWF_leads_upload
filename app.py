# -*- coding: utf-8 -*-
"""
Created on Tue Jul 30 12:01:35 2024

@author: Rik
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Jul 22 13:26:47 2024

@author: Rik
"""

import json
import os
import io
import logging
import requests
from flask import Flask, request
import pandas as pd
from google.auth import default
from googleapiclient.discovery import build
import sys
import traceback
from google.cloud import logging as cloud_logging
from google.cloud import storage
from googleapiclient.http import MediaIoBaseDownload


app = Flask(__name__)

def setup_google_cloud_logging():
    # Instantiates a client
    client = cloud_logging.Client()

    # Retrieves a Cloud Logging handler based on the environment
    # and integrates the handler with the Python logging module.
    # This captures all logs at INFO level and higher.
    client.setup_logging()

    # You can still set up additional handlers for local output
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    # Get the root logger and attach both handlers
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)

    return root_logger

# Initialize the logger
logger = setup_google_cloud_logging()

# Correcting the exception handling to log with severity
def log_uncaught_exceptions(ex_cls, ex, tb):
    logging.error(''.join(traceback.format_tb(tb)))
    logging.error(f'{ex_cls.__name__}: {str(ex)}')

sys.excepthook = log_uncaught_exceptions
# Get the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Path to config.json
config_path = os.path.join(script_dir, 'config.json')

# Load configuration from JSON file
with open(config_path) as config_file:
    config = json.load(config_file)

# API Base URL
api_url = config['api_url']

# Common API parameters that are always included
common_api_params = config['common_api_params']

# Mapping from CSV headers to API field names
allowed_fields = config['allowed_fields']

def upload_lead(lead_data):
    
    # Remove spaces from field names
    lead_data = {k.replace(' ', ''): v for k, v in lead_data.items()}

    # Ensure the leading zero in the phone number is preserved and set phone_number field
    if 'TelefoonnrPrive1' in lead_data:
        phone_number = str(lead_data['TelefoonnrPrive1']).zfill(10)
        lead_data['TelefoonnrPrive1'] = phone_number
        lead_data['phone_number'] = phone_number
        
    if 'OvereenkomstBedragPerPeriode' in lead_data:
        lead_data['Oudbedragcustom'] = lead_data['OvereenkomstBedragPerPeriode']


    filtered_data = {k: v for k, v in lead_data.items() if k in allowed_fields and pd.notna(v)}

    # Ensure phone_number is included in the API call
    if 'phone_number' in lead_data:
        filtered_data['phone_number'] = lead_data['phone_number']
        
        
    api_params = {**common_api_params, **filtered_data}

    response = requests.get(api_url, params=api_params)
    print(f"Response Status: {response.status_code}, Response Text: {response.text}")

    if response.ok and 'ERROR' not in response.text:
        return {'success': True, 'data': lead_data}
    else:
        error_message = response.text
        return {'success': False, 'data': lead_data, 'error': error_message}

def process_and_upload_leads(df):
    leads = df.to_dict(orient='records')
    success_count = 0
    error_count = 0

    for lead in leads:
        result = upload_lead(lead)
        if result['success']:
            success_count += 1
        else:
            error_count += 1
            logging.error(f"Lead upload failed: Data: {result.get('data')}, Error Message: {result.get('error')}")

    logging.info(f"Total leads uploaded successfully: {success_count}")
    logging.info(f"Total leads failed to upload: {error_count}")
    
    
def build_drive_service():
    # Define the scopes required for the Google Drive service
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

    # Obtain default credentials for the Cloud Run environment
    credentials, _ = default(scopes=scopes)

    # Explicitly request the credentials to refresh if they are not already valid
    if not credentials.valid:
        if credentials.requires_scopes:
            credentials = credentials.with_scopes(scopes)    
    # Build the service client using the obtained credentials
    service = build('drive', 'v3', credentials=credentials)
    return service

def get_latest_file(service, folder_id, prefix):
    """Fetch the latest file from Google Drive with the specified prefix in the given folder."""
    query = f"'{folder_id}' in parents and name contains '{prefix}' and trashed = false"
    results = service.files().list(
        q=query,
        spaces='drive',
        fields='files(id, name, modifiedTime)',
        orderBy='modifiedTime desc',  # Order by last modified time descending
        pageSize=1,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True# We only need the most recent file
    ).execute()
    items = results.get('files', [])
    if not items:
        logging.error("No files found.")
        return None, None
    else:
        latest_file = items[0]
        logging.info(f"Latest file found: {latest_file['name']} with ID: {latest_file['id']}")
        # Download the file content using the file ID if necessary or return its ID
        request = service.files().get_media(fileId=latest_file['id'],supportsAllDrives=True)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        fh.seek(0)
        return fh, latest_file['name']

def is_file_processed(file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket('automatic_processes_bucket')  # Replace with your GCS bucket name
    blob = bucket.blob(f'KWF_leads/processed_files/{file_name}')
    return blob.exists()

def record_processed_file(file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket('automatic_processes_bucket')  # Replace with your GCS bucket name
    blob = bucket.blob(f'KWF_leads/processed_files/{file_name}')
    blob.upload_from_string('')  # Upload an empty string as a marker

def main():
    logging.info("Starting the application.")
    service = build_drive_service()
    folder_id = config.get('folder_id')
    latest_file_content, latest_file_name = get_latest_file(service, folder_id, "KWF-D2D-KWFexport")

    if latest_file_content and not is_file_processed(latest_file_name):
        df = pd.read_csv(latest_file_content, delimiter=';')
        process_and_upload_leads(df)
        record_processed_file(latest_file_name)
    else:
        logging.warning(f"File {latest_file_name} has already been processed.")

@app.route('/')
def run_main():
    logging.info("Received request at '/' endpoint")
    try:
        main()
    except Exception as e:
        logging.error("Error occurred during main execution", trace=traceback.format_exc())
        return "Internal Server Error", 500
    return "Script executed successfully."

if __name__ == '__main__':
    logging.info("Starting Flask application.")
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=True)
