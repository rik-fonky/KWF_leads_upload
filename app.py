# -*- coding: utf-8 -*-
"""
Created on Tue Jul 30 12:01:35 2024

@author: Rik
"""
import concurrent.futures
import json
import os
import io
import logging
import requests
from flask import Flask
import pandas as pd
from google.auth import default
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import sys
import traceback
from google.cloud import logging as cloud_logging
from google.cloud import secretmanager
import chardet
from collections import defaultdict
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time
from paramiko import Transport, SFTPClient
from fnmatch import fnmatch


session = None

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

credentials_sftp_json = access_secret_version("benw-sftp-credentials")

# Parse the credentials (assuming they are in JSON format)
credentials = json.loads(credentials_json)

credentials_sftp = json.loads(credentials_sftp_json)

# API Base URL
api_url = config['api_url']

# Common API parameters that are always included
common_api_params = config['common_api_params']

# Merge credentials into the config dictionary
common_api_params.update(credentials)


# Mapping from CSV headers to API field names
allowed_fields = config['allowed_fields']

def chunk_dataframe(df, chunk_size):
    """Yield successive chunks from the DataFrame."""
    for i in range(0, len(df), chunk_size):
        yield df.iloc[i:i + chunk_size]
        
def setup_global_session(retries=3, backoff_factor=1.0, status_forcelist=(500, 502, 503, 504)):
    """
    Set up a global session with retry and connection pooling.
    """
    global session  # Use the global variable
    session = requests.Session()  # Create a session object
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(['GET', 'POST'])  # Ensure retrying on GET and POST requests
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

# Call the function to set up the session at the start
setup_global_session()

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

    response = session.get(api_url, params=api_params, timeout=20)
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

def clean_phone_number(phone_number):
    phone_str = str(phone_number).strip()
    if phone_str.endswith('.0'):
        phone_str = phone_str[:-2]
    if phone_str.startswith('316'):
        return phone_str[2:]
    elif phone_str.startswith('0'):
        return phone_str[1:]
    return phone_str



def get_latest_file_sftp():
    try:
        # Establish SFTP connection
        SFTP_HOST = credentials_sftp["sftp_host"]
        SFTP_PORT = int(credentials_sftp["sftp_port"])
        SFTP_USERNAME = credentials_sftp["sftp_username"]
        SFTP_PASSWORD = credentials_sftp["sftp_password"]
        SFTP_DIRECTORY = credentials_sftp["sftp_directory"]
        
        
        transport = Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
        sftp = SFTPClient.from_transport(transport)
        
        # Construct file name pattern to match
        file_pattern = f"KWF-D2D*.csv"
        
        # Debugging output
        print(f"Searching for files matching pattern: {file_pattern}")
        
        # Get list of files in SFTP directory
        files_in_directory = sftp.listdir_attr(SFTP_DIRECTORY)
        
        # Filter files matching the pattern and find the latest file by modification time
        latest_file = None
        latest_mtime = 0
        for file_info in files_in_directory:
            if fnmatch(file_info.filename, file_pattern):
                if file_info.st_mtime > latest_mtime:
                    latest_file = file_info
                    latest_mtime = file_info.st_mtime

        # If no matching file is found, return an empty DataFrame and None
        if not latest_file:
            print("No matching files found.")
            return pd.DataFrame(), None

        # Open the latest file and process it
        latest_file_path = f"{SFTP_DIRECTORY.rstrip('/')}/{latest_file.filename}"
        print(f"Latest file found: {latest_file.filename}, modified at {latest_file.st_mtime}")
        
        with sftp.open(latest_file_path) as file:
            df = pd.read_csv(file, sep=';')
            if 'Telefoonnr Prive1' in df.columns:
                # Clean Telefoon column: remove non-numeric characters and leading zeros
                df['Telefoonnr Prive1'] = df['Telefoonnr Prive1'].fillna('-1').astype(str).apply(clean_phone_number)

        # Close SFTP connection
        sftp.close()
        transport.close()
        
        # Return the DataFrame and filename
        return df, latest_file.filename

    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame(), None  # Return an empty DataFrame and None on error


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


def optin_velden_fix(leads_df):
    
    leads_df.loc[leads_df["Extraveld1 - opt-in Tel"] == "Yes", "MutOptOutTM"] = "N"
    leads_df.loc[leads_df["Extraveld1 - opt-in Tel"] == "No", "MutOptOutTM"] = "Y"
    
    leads_df.loc[leads_df["Extraveld2 - opt-in SMS"] == "Yes", "MutOptOutSMS"] = "N"
    leads_df.loc[leads_df["Extraveld2 - opt-in SMS"] == "No", "MutOptOutSMS"] = "Y"
    
    leads_df.loc[leads_df["Extraveld3 - opt-in E-mail"] == "Yes", "MutOptOutEM"] = "N"
    leads_df.loc[leads_df["Extraveld3 - opt-in E-mail"] == "No", "MutOptOutEM"] = "Y"
    
    
    return leads_df


def main():
    logging.info("Starting the application.")
    df_latest_file, latest_file_name = get_latest_file_sftp()
    logging.info(latest_file_name)
    if not df_latest_file.empty and not is_file_processed(latest_file_name):
        df_leads = optin_velden_fix(df_latest_file)
        process_and_upload_leads(df_leads)
        record_processed_file(df_leads)
    else:
        logging.warning(f"File {latest_file_name} has already been processed.")

    return latest_file_name

@app.route('/')
def run_main():
    logging.info("Received request at '/' endpoint")
    try:
        files_processed = main()
    except Exception as e:
        logging.error("Error occurred", exc_info=True)
        return "Internal Server Error", 500 
    return f"Script executed successfully, with the following processed files {files_processed}"

if __name__ == '__main__':
    logging.info("Starting Flask application.")
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=True)
