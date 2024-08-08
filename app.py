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
import tempfile
import logging
from datetime import datetime
import requests
from flask import Flask, request
import pandas as pd
import concurrent.futures
import paramiko
from google.auth import default
from google.oauth2 import service_account
from google.cloud import storage, secretmanager
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Path to config.json
config_path = os.path.join(script_dir, 'config.json')

# Load configuration from JSON file
with open(config_path) as config_file:
    config = json.load(config_file)

log_file_prefix = config['log_file_prefix']

# Generate log file name with datetime
current_time = datetime.now().strftime("%Y%d%m_%H%M%S")
log_file = f'{log_file_prefix}_{current_time}.txt'

# Create and configure the application logger
app_logger = logging.getLogger('app_logger')
app_logger.setLevel(logging.DEBUG)  # Set to DEBUG level

# Create file handler which logs even debug messages
file_handler = logging.FileHandler(log_file, mode='w')
file_handler.setLevel(logging.DEBUG)  # Set to DEBUG level

# Create console handler with a higher log level
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)  # Set to DEBUG level

# Create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
app_logger.addHandler(file_handler)
app_logger.addHandler(console_handler)

# Separate logger for HTTP server access logs
http_logger = logging.getLogger('werkzeug')
http_logger.setLevel(logging.INFO)
http_handler = logging.StreamHandler()
http_handler.setFormatter(formatter)
http_logger.addHandler(http_handler)

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
    error_details = []

    for lead in leads:
        result = upload_lead(lead)
        if result['success']:
            success_count += 1
        else:
            error_count += 1
            error_details.append(result)

    app_logger.info(f'Total leads uploaded successfully: {success_count}')
    app_logger.info(f'Total leads failed to upload: {error_count}')
    if error_details:
        app_logger.error('Error details:')
        for error in error_details:
            app_logger.error(f'Data: {error.get("data")}, Error Message: {error.get("error")}')

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

def upload_file_to_drive(service, file_name, folder_id):
    app_logger.info(f"Attempting to upload file to folder ID: {folder_id}")
    file_metadata = {
        'name': file_name,
        'parents': [folder_id]
    }
    media = MediaFileUpload(file_name, mimetype='text/plain')
    try:
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id',
            supportsAllDrives=True).execute()
        app_logger.info(f"File ID: {file['id']} uploaded to Google Drive.")
        return file['id']
    except HttpError as error:
        app_logger.error(f"An error occurred: {error}")
        return None

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
        app_logger.error('No files found.')
        return None, None
    else:
        latest_file = items[0]
        app_logger.info(f"Latest file found: {latest_file['name']} with ID: {latest_file['id']}")
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
    app_logger.info("Starting the application.")
    
    # Setup Google Drive service
    service = build_drive_service()
    
    # Fetch the latest file
    folder_id = "1EnF0Ak4tcI3s3ExWDic1uDtfLT73JKFP"  # Ensure you have the correct folder ID
    latest_file_content, latest_file_name = get_latest_file(service, folder_id, "KWF-D2D-KWFexport")
    
    if latest_file_content and not is_file_processed(latest_file_name):
        df = pd.read_csv(latest_file_content, delimiter=';')
        process_and_upload_leads(df)
        record_processed_file(latest_file_name)
    else:
        print(f"File {latest_file_name} has already been uploaded")
        app_logger.info(f"File {latest_file_name} has already been uploaded")

    # Verify the folder ID
    folder_id = config['folder_id']
    app_logger.info(f"Using folder ID: {folder_id}")
    
    # Upload the log file to Google Drive
    log_file_id = upload_file_to_drive(service, log_file, folder_id)
    
    if log_file_id:
        app_logger.info(f"Log file uploaded with file ID: {log_file_id}")
    else:
        app_logger.error("Failed to upload log file.")
    
    # Flush and close all handlers
    for handler in app_logger.handlers:
        handler.flush()
        handler.close()

@app.route('/')
def run_main():
    app_logger.info("Received request at '/' endpoint")
    try:
        main()
    except Exception as e:
        app_logger.error("Error occurred", exc_info=True)
        return "Internal Server Error", 500
    return "Script executed successfully."

if __name__ == '__main__':
    print("Starting Flask application.")
    port = int(os.environ.get('PORT', 8080))
    app.debug = True
    app.run(host='0.0.0.0', port=port)
