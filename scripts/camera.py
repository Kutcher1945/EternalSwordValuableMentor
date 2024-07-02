import requests
import json
import time
from tqdm import tqdm
from retrying import retry

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

# Function to refresh token every minute
def refresh_token(url, login, password):
    while True:
        token = get_token(url, login, password)
        if token:
            yield token
            time.sleep(60)  # Wait for 60 seconds before refreshing the token
        else:
            print("Token refresh failed. Exiting.")
            return None

# Retry decorator with exponential backoff for API requests
@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
def get_cameras_data_with_retry(url, token, cursor=None):
    headers = {"Authorization": f"Bearer {token}"}
    params = {"cursor": cursor} if cursor else None
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 503:
        print("Retry after 503 error")
        response.raise_for_status()
    elif response.status_code == 401:
        print("Received 401 Unauthorized error. Attempting to obtain a new token.")
        new_token = next(token_generator)
        if new_token:
            print("New token obtained successfully.")
            return get_cameras_data_with_retry(url, new_token, cursor)
        else:
            print("Failed to obtain a new token. Exiting.")
            response.raise_for_status()
    else:
        print("Failed to get cameras data. Status code:", response.status_code)
        response.raise_for_status()

# Function to save data to JSON or GeoJSON file
def save_to_file(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f)

# Function to get all cameras data with pagination support
def get_all_cameras_data(url, token):
    all_cameras_data = []
    cursor = None
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
            except Exception as e:
                print("An error occurred:", str(e))
                break
    return all_cameras_data

# URL and credentials to obtain initial token
token_url = "https://esvm.kz/api/v1/token"
login = "cra_api@esvm.kz"
password = "qyKoZ7wosJf2W7AhOFINz5clCyOdKtD0"

# URL to get cameras data
cameras_url = "https://esvm.kz/api/v1/stats/cameras/"

# Generator to refresh token every minute
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
        # You can also convert to GeoJSON format if needed
        # save_to_file(all_cameras_data, "cameras_data.geojson")
        # print("Cameras data saved to cameras_data.geojson")
    else:
        print("Unable to save cameras data.")
else:
    print("Token not obtained. Cannot retrieve cameras data.")
