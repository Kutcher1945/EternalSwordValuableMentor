import requests
import json
from datetime import datetime, timedelta

# Function to obtain token
def get_token(url, login, password):
    payload = {
        "grant_type": "password",
        "username": login,
        "password": password
    }
    response = requests.post(url, data=payload)
    if response.status_code == 200:
        data = response.json()
        return data.get("access_token")
    else:
        print("Failed to obtain token. Status code:", response.status_code)
        return None

# Function to get events data
def get_events_data(url, token, ts_from, ts_to, event_type, only_matches=False, stream_id=None, place_id=None, list_id=None, group_id=None, limit=20):
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "ts_from": ts_from,
        "ts_to": ts_to,
        "event_type": event_type,
        "only_matches": only_matches,
        "stream_id": stream_id,
        "place_id": place_id,
        "list_id": list_id,
        "group_id": group_id,
        "limit": limit
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to get events data. Status code:", response.status_code)
        print("Response content:", response.content)
        return None

# URL and credentials to obtain initial token
token_url = "https://esvm.kz/api/v1/token"
login = "cra_api@esvm.kz"
password = "qyKoZ7wosJf2W7AhOFINz5clCyOdKtD0"

# Obtain token
token = get_token(token_url, login, password)

# Check if token obtained successfully
if token:
    # Define time range for events
    start_time = datetime.now() - timedelta(hours=24)  # Start time, 24 hours ago from current time
    end_time = datetime.now()  # End time, current time

    # Format timestamps in ISO 8601 format
    ts_from = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    ts_to = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Get events data
    events_url = "https://esvm.kz/api/v1/stats/cameras"
    event_type = "ao"  # Specify the event type
    events_data = get_events_data(events_url, token, ts_from, ts_to, event_type)

    # Check if events data obtained successfully
    if events_data:
        # Print data obtained from the API
        print("Events data:")
        print(json.dumps(events_data, indent=4))
        
        # Save data to JSON file with UTF-8 encoding
        with open("plates_data.json", "w", encoding="utf-8") as file:
            json.dump(events_data, file, indent=4, ensure_ascii=False)
            print("plates data saved to plates_data.json")
    else:
        print("Unable to fetch events data.")
else:
    print("Token not obtained. Cannot retrieve events data.")
