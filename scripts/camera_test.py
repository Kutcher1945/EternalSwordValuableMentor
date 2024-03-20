import requests
import json
import time
from tqdm import tqdm
from retrying import retry
import jwt

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

# Function to refresh token only if it's about to expire or has expired
def refresh_token(url, login, password):
    while True:
        token = get_token(url, login, password)
        if token:
            try:
                token_data = jwt.decode(token, options={"verify_signature": False})
                print("New token obtained:", token)  # Print new token
                yield token
                expiration_time = token_data.get("exp")
                if expiration_time:
                    current_time = time.time()
                    time_to_expire = expiration_time - current_time
                    if time_to_expire > 1800:  # Token expires every 30 minutes (1800 seconds)
                        time_to_sleep = time_to_expire - 1800  # Sleep until 30 seconds before expiration
                        print(f"Sleeping for {time_to_sleep} seconds until token expiration.")
                        time.sleep(time_to_sleep)
            except jwt.exceptions.DecodeError as e:
                print("Invalid token received. Retrying in 30 seconds...")
                time.sleep(30)  # Retry after 30 seconds if token decoding failed
        else:
            print("Token refresh failed. Retrying in 30 seconds...")
            time.sleep(30)  # Retry after 30 seconds if token retrieval failed

# Retry decorator with exponential backoff for API requests
@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
def get_cameras_data_with_retry(url, token, cursor=None):
    headers = {"Authorization": f"Bearer {token}"}
    params = {"cursor": cursor} if cursor else None
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()  # Raise exception for non-200 status codes
    return response.json()

# Function to save data to JSON or GeoJSON file
def save_to_file(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f)

# Function to get all cameras data with pagination support
def get_all_cameras_data(url, token, start_cursor=None):
    all_cameras_data = []
    cursor = start_cursor
    with tqdm(desc="Fetching Cameras Data", unit=" pages") as progress_bar:
        while True:
            try:
                cameras_data = get_cameras_data_with_retry(url, token, cursor)
                if cameras_data:
                    all_cameras_data.extend(cameras_data)
                    if "pagination" in cameras_data and "cursor" in cameras_data["pagination"]:
                        cursor = cameras_data["pagination"]["cursor"]
                        progress_bar.update(1)
                    else:
                        break
                else:
                    break
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    print("Received 401 Unauthorized error. Attempting to obtain a new token.")
                    new_token = next(token_generator)
                    if new_token:
                        print("New token obtained successfully:", new_token)  # Print new token
                        print("Using new token.")  # Print message when using new token
                        token = new_token
                        continue  # Continue fetching data with the new token
                    else:
                        print("Failed to obtain a new token. Exiting.")
                        break
                else:
                    print("Failed to get cameras data. Status code:", e.response.status_code)
                    break
            except Exception as e:
                print("An error occurred:", str(e))
                break
    return all_cameras_data

# URL and credentials to obtain initial token
token_url = "https://esvm.kz/api/v1/token"
login = "cra_api@esvm.kz"
password = "qyKoZ7wosJf2W7AhOFINz5clCyOdKtD0"

# URL to get cameras data
cameras_url = "https://esvm.kz/api/v1/cameras"

# Generator to refresh token only if it's about to expire or has expired
token_generator = refresh_token(token_url, login, password)

# Obtain initial token
token = next(token_generator)

# Check if token obtained successfully
if token:
    # Get all cameras data with pagination support
    all_cameras_data = get_all_cameras_data(cameras_url, token)

    # Check if cameras data obtained successfully
    if all_cameras_data:
        # Save data to JSON file
        save_to_file(all_cameras_data, "cameras_data.json")
        print("Cameras data saved to cameras_data.json")
    else:
        print("Unable to save cameras data.")
else:
    print("Token not obtained. Cannot retrieve cameras data.")
