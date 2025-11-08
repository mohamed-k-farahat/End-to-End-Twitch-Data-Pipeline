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

    # --- 3. API Call 1: Get Top Streams (with pagination to get 5000) ---
    streams_url = 'https://api.twitch.tv/helix/streams'
    streams_data = []
    cursor = None
    target_streams = 1000  # Reduced from 5000 to avoid Azure upload timeouts
    max_pages = 10  # 10 pages × 100 = 1000 streams
    
    print(f"Fetching up to {target_streams} live streams...")
    
    for page in range(max_pages):
        params_streams = {'first': 100}
        if cursor:
            params_streams['after'] = cursor
        
        response = requests.get(streams_url, headers=headers, params=params_streams)
        response.raise_for_status()
        page_data = response.json()
        
        data = page_data.get('data', [])
        streams_data.extend(data)
        
        # Get pagination cursor for next page
        cursor = page_data.get('pagination', {}).get('cursor')
        
        print(f"Page {page + 1}: Fetched {len(data)} streams (Total: {len(streams_data)})")
        
        # Stop if no more data or reached target
        if not cursor or len(streams_data) >= target_streams:
            break
    
    print(f"Successfully fetched {len(streams_data)} live streams.")
    
    if not streams_data:
        print("No live streams found. Exiting.")
        return

    # --- 4. Process IDs for Batch Calls ---
    user_ids = list({stream['user_id'] for stream in streams_data})
    game_ids = list({stream['game_id'] for stream in streams_data if stream.get('game_id')})

    # --- 5. API Call 2: Get User Details (Batched in chunks of 100) ---
    # Twitch API allows max 100 user IDs per request
    users_url = 'https://api.twitch.tv/helix/users'
    users_data = []
    batch_size = 100
    
    print(f"Fetching details for {len(user_ids)} users in batches of {batch_size}...")
    
    for i in range(0, len(user_ids), batch_size):
        batch = user_ids[i:i + batch_size]
        params_users = [('id', user_id) for user_id in batch]
        
        response = requests.get(users_url, headers=headers, params=params_users)
        response.raise_for_status()
        batch_data = response.json().get('data', [])
        users_data.extend(batch_data)
        
        print(f"  Fetched {len(batch_data)} users (Total: {len(users_data)}/{len(user_ids)})")
    
    print(f"Successfully fetched {len(users_data)} user details.")

    # --- 6. API Call 3: Get Game Details (Batched in chunks of 100) ---
    # Twitch API allows max 100 game IDs per request
    games_url = 'https://api.twitch.tv/helix/games'
    games_data = []
    
    print(f"Fetching details for {len(game_ids)} games in batches of {batch_size}...")
    
    for i in range(0, len(game_ids), batch_size):
        batch = game_ids[i:i + batch_size]
        params_games = [('id', game_id) for game_id in batch]
        
        response = requests.get(games_url, headers=headers, params=params_games)
        response.raise_for_status()
        batch_data = response.json().get('data', [])
        games_data.extend(batch_data)
        
        print(f"  Fetched {len(batch_data)} games (Total: {len(games_data)}/{len(game_ids)})")
    
    print(f"Successfully fetched {len(games_data)} game details.")

    # --- 7. Save Data to JSON (in memory) ---
    # Use UTC timestamp to match Twitch API timezone (timezone-aware)
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_list = [
        {'blob_path': f'streams/streams_{timestamp}.json', 'data': json.dumps(streams_data, indent=2)},
        {'blob_path': f'users/users_{timestamp}.json',   'data': json.dumps(users_data, indent=2)},
        {'blob_path': f'games/games_{timestamp}.json',   'data': json.dumps(games_data, indent=2)}
    ]
    print(f"Preparing JSON files with UTC timestamp: {timestamp}")
    print(f"  Streams: {len(streams_data)} records")
    print(f"  Users: {len(users_data)} records")
    print(f"  Games: {len(games_data)} records")
    
    # Compact JSON (no indentation) to reduce file size for faster uploads
    file_list = [
        {'blob_path': f'streams/streams_{timestamp}.json', 'data': json.dumps(streams_data), 'type': 'streams'},
        {'blob_path': f'users/users_{timestamp}.json',   'data': json.dumps(users_data), 'type': 'users'},
        {'blob_path': f'games/games_{timestamp}.json',   'data': json.dumps(games_data), 'type': 'games'}
    ]
    
    print(f"JSON data serialized successfully.")

    # --- 8. Upload to Azure Blob Storage ---
    print("Starting upload to Azure Blob Storage...")
    
    # Increase timeout to 300 seconds (5 minutes) per file to handle large uploads
    blob_service_client = BlobServiceClient.from_connection_string(
        AZURE_CONN_STRING,
        connection_timeout=300,
        read_timeout=300
    )
    
    for file in file_list:
        file_size_mb = len(file['data']) / (1024 * 1024)
        print(f"  Uploading {file['type']}: {file['blob_path']} ({file_size_mb:.2f} MB)...")
        
        blob_client = blob_service_client.get_blob_client(container=AZURE_CONTAINER_NAME, blob=file['blob_path'])
        blob_client.upload_blob(file['data'], overwrite=True, timeout=300)
        
        print(f"  ✓ Successfully uploaded {file['type']} to Azure.")

    print("=" * 60)
    print("✓ Extraction and Load to Azure complete!")
    print(f"  Total streams: {len(streams_data)}")
    print(f"  Total users: {len(users_data)}")
    print(f"  Total games: {len(games_data)}")
    print("=" * 60)

# This allows you to still test the script manually if needed,
# but Airflow will import it as a module.
if __name__ == "__main__":
    fetch_and_load_to_blob()