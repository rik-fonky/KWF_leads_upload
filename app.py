# -*- coding: utf-8 -*-
"""
Created on Tue Jul 30 12:01:35 2024

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
from google.cloud import secretmanager
import concurrent.futures


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


def access_secret_version(secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/637358609369/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    secret_payload = response.payload.data.decode("UTF-8")
    return secret_payload


# Fetch credentials from Secret Manager
credentials_json = access_secret_version("telforce_api_credentials")

# Parse the credentials (assuming they are in JSON format)
credentials = json.loads(credentials_json)

# API Base URL
api_url = config['api_url']

# Common API parameters that are always included
common_api_params = config['common_api_params']

# Merge credentials into the config dictionary
common_api_params.update(credentials)

print(common_api_params)

# Mapping from CSV headers to API field names
allowed_fields = config['allowed_fields']


def upload_lead(lead_data):
    # Remove spaces from field names
    lead_data.index = lead_data.index.str.replace(' ', '')

    # Ensure the leading zero in the phone number is preserved and set phone_number field
    if 'TelefoonnrPrive1' in lead_data:
        # Replace NaN with default value
        if pd.isna(lead_data['TelefoonnrPrive1']):
            lead_data['TelefoonnrPrive1'] = '0600000000'
            
            
        lead_data['TelefoonnrPrive1'] = str(lead_data['TelefoonnrPrive1'])
        lead_data['phone_number'] = str(lead_data['TelefoonnrPrive1'])
        
    if 'OvereenkomstBedragPerPeriode' in lead_data:
        lead_data['Oudbedragcustom'] = lead_data['OvereenkomstBedragPerPeriode']

    # Convert Series to dictionary for filtering
    lead_data_dict = lead_data.to_dict()

    filtered_data = {k: v for k, v in lead_data_dict.items() if k in allowed_fields and pd.notna(v)}

    # Ensure phone_number is included in the API call
    if 'phone_number' in lead_data:
        filtered_data['phone_number'] = lead_data_dict['phone_number']
        
        
    api_params = {**common_api_params, **filtered_data}

    response = requests.get(api_url, params=api_params)
    logging.info(f"Response Status: {response.status_code}, Response Text: {response.text}")

    if response.ok and 'ERROR' not in response.text:
        return {'success': True, 'data': lead_data}
    else:
        error_message = response.text
        return {'success': False, 'data': lead_data, 'error': error_message}

def process_and_upload_leads(df, chunk_size=20):
    success_count = 0
    error_count = 0
    errors = defaultdict(list)

    # Break the DataFrame into chunks
    for chunk in chunk_dataframe(df, chunk_size=chunk_size):
        logging.info(f"Processing chunk with {len(chunk)} leads")

        # Create a thread pool for each chunk
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(upload_lead, row) for index, row in chunk.iterrows()]
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    if result['success']:
                        success_count += 1
                    else:
                        error_count += 1
                        error_parts = result['error'].split(' - ')
                        generic_error_msg = error_parts[0]
                        detail = error_parts[1].strip() if len(error_parts) > 1 else "No specific detail"
                        errors[generic_error_msg].append(detail)
                except Exception as e:
                    error_count += 1
                    errors['Exception occurred'].append(str(e).strip())

        logging.info(f"Finished processing chunk. Success: {success_count}, Errors: {error_count}")
        
        time.sleep(1)  # Introduce a short pause to prevent overwhelming the API server

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
