# File: dags/twitch_extraction.py

import requests
import json
import datetime
import os
from azure.storage.blob import BlobServiceClient
from airflow.models import Variable
from airflow.hooks.base import BaseHook

def get_twitch_access_token(client_id, client_secret):
    """Gets an OAuth App Access Token from Twitch."""
    auth_url = 'https://id.twitch.tv/oauth2/token'
    params = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }
    response = requests.post(auth_url, params=params)
    response.raise_for_status() # Will raise an error if auth fails
    return response.json()['access_token']

def fetch_and_load_to_blob():
    """
    Main ETL function to be called by Airflow.
    Extracts from Twitch and loads raw JSON files into Azure Blob Storage.
    """
    print("Starting Twitch data extraction...")
    
    # --- 1. Get Credentials from Airflow ---
    # Get Variables
    CLIENT_ID = Variable.get("TWITCH_CLIENT_ID")
    CLIENT_SECRET = Variable.get("TWITCH_CLIENT_SECRET")
    
    # Get Azure Connection String from the Airflow Connection
    azure_conn = BaseHook.get_connection("azure_blob_conn")
    # We must parse the JSON "Extra" field
    azure_conn_extra = azure_conn.extra_dejson
    AZURE_CONN_STRING = azure_conn_extra.get("connection_string")
    AZURE_CONTAINER_NAME = "twitch-raw-data" 

    if not CLIENT_ID or not CLIENT_SECRET or not AZURE_CONN_STRING:
        raise ValueError("Airflow Connections/Variables are not set correctly.")

    print("Successfully retrieved credentials from Airflow.")

    # --- 2. Get Twitch Access Token ---
    access_token = get_twitch_access_token(CLIENT_ID, CLIENT_SECRET)
    headers = {
        'Client-ID': CLIENT_ID,
        'Authorization': f'Bearer {access_token}'
    }
    print("Twitch access token received.")

    # --- 3. API Call 1: Get Top Streams ---
    streams_url = 'https://api.twitch.tv/helix/streams'
    params_streams = {'first': 100}
    response = requests.get(streams_url, headers=headers, params=params_streams)
    response.raise_for_status()
    streams_data = response.json().get('data', [])
    print(f"Fetched {len(streams_data)} live streams.")
    
    if not streams_data:
        print("No live streams found. Exiting.")
        return

    # --- 4. Process IDs for Batch Calls ---
    user_ids = {stream['user_id'] for stream in streams_data}
    game_ids = {stream['game_id'] for stream in streams_data if stream.get('game_id')}

    # --- 5. API Call 2: Get User Details (Batch) ---
    users_url = 'https://api.twitch.tv/helix/users'
    params_users = [('id', user_id) for user_id in user_ids]
    response = requests.get(users_url, headers=headers, params=params_users)
    response.raise_for_status()
    users_data = response.json().get('data', [])
    print(f"Fetched {len(users_data)} user details.")

    # --- 6. API Call 3: Get Game Details (Batch) ---
    games_url = 'https://api.twitch.tv/helix/games' # <-- This line is now fixed
    params_games = [('id', game_id) for game_id in game_ids]
    response = requests.get(games_url, headers=headers, params=params_games)
    response.raise_for_status()
    games_data = response.json().get('data', [])
    print(f"Fetched {len(games_data)} game details.")

    # --- 7. Save Data to JSON (in memory) ---
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_list = [
        {'blob_path': f'streams/streams_{timestamp}.json', 'data': json.dumps(streams_data, indent=2)},
        {'blob_path': f'users/users_{timestamp}.json',   'data': json.dumps(users_data, indent=2)},
        {'blob_path': f'games/games_{timestamp}.json',   'data': json.dumps(games_data, indent=2)}
    ]
    print(f"JSON data prepared with timestamp: {timestamp}")

    # --- 8. Upload to Azure Blob Storage ---
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONN_STRING)
    
    for file in file_list:
        blob_client = blob_service_client.get_blob_client(container=AZURE_CONTAINER_NAME, blob=file['blob_path'])
        blob_client.upload_blob(file['data'], overwrite=True)
        print(f"Successfully uploaded {file['blob_path']} to Azure.")

    print("Extraction and Load to Azure complete.")

# This allows you to still test the script manually if needed,
# but Airflow will import it as a module.
if __name__ == "__main__":
    fetch_and_load_to_blob()