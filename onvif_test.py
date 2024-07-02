import psycopg2
import requests
import time
import json
from retrying import retry
from tqdm import tqdm

# Database connection parameters
DB_HOST = '172.30.227.205'
DB_PORT = '5439'
DB_NAME = 'sitcenter_postgis_datalake'
DB_USER = 'la_noche_estrellada'
DB_PASSWORD = 'Cfq,thNb13@'  # Define DB_PASSWORD variable

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

def get_camera_info(camera_id, base_url, token):
    # Fetch necessary credentials from the database
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Query to select onvif_login and onvif_password for the given camera_id
        select_query = "SELECT onvif_host, onvif_port, onvif_login, onvif_password FROM esvm_cameras WHERE camera_id = %s AND onvif_host IS NOT NULL"

        cursor.execute(select_query, (camera_id,))
        camera_info = cursor.fetchone()

        if camera_info:
            onvif_host, onvif_port, onvif_login, onvif_password = camera_info
            url = f"{base_url}/api/v1/onvif/{camera_id}"
            auth = (onvif_login, onvif_password)  # Provide authentication credentials
            response = requests.get(url, auth=auth)
            print(f"Login: {onvif_login}, Password: {onvif_password}")

            if response.status_code == 200:
                # Camera information retrieved successfully
                camera_info = response.json()
                return camera_info
            elif response.status_code == 400:
                # Handle bad request errors
                error_message = response.json().get("message", "Bad request")
                print(f"Error for camera ID {camera_id}: Bad request - {error_message}")
            elif response.status_code == 404:
                # Handle not found errors
                error_message = response.json().get("message", "Not found")
                print(f"Error for camera ID {camera_id}: Not found - {error_message}")
            else:
                # Handle other errors
                print(f"Error for camera ID {camera_id}: Status code - {response.status_code}")
                print("Response content:", response.content)  # Print response content for debugging
        else:
            print(f"No credentials found for camera ID {camera_id}")

    except Exception as e:
        print("Failed to fetch camera info:", str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()



def parse_camera_info_from_api(token):
    # Assuming DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD are defined somewhere

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Query to select camera IDs from the esvm_cameras table
        select_query = "SELECT camera_id FROM esvm_cameras WHERE onvif_host IS NOT NULL"

        cursor.execute(select_query)
        camera_ids = cursor.fetchall()

        for camera_id in camera_ids:
            camera_id = camera_id[0]  # Extracting camera_id from the tuple
            camera_info = get_camera_info(camera_id, base_url="https://esvm.kz", token=token)  # Update base_url as needed
            # Handle camera_info as needed, for example, insert into another table or process further

    except Exception as e:
        print("Failed to parse camera info:", str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Obtain initial token
token = get_token("https://esvm.kz/api/v1/token", login="cra_api@esvm.kz", password="qyKoZ7wosJf2W7AhOFINz5clCyOdKtD0")

if token:
    # Call the function to parse camera info from the API
    parse_camera_info_from_api(token)
else:
    print("Token not obtained. Cannot retrieve camera information.")
