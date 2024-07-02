import requests
import psycopg2
from psycopg2.extras import Json
import time
from tqdm import tqdm

# Database connection parameters
DB_HOST = '172.30.227.205'
DB_PORT = '5439'
DB_NAME = 'sitcenter_postgis_datalake'
DB_USER = 'la_noche_estrellada'
DB_PASSWORD = 'Cfq,thNb13@'

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

# Function to get stream data
def get_stream_data(url, token):
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 404:
        return None  # Indicate that the stream was not found
    else:
        print("Failed to get stream data. Status code:", response.status_code)
        print("Response content:", response.content)
        return None

# Function to mark camera as inactive
def mark_camera_as_inactive(camera_id):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        update_query = """
        INSERT INTO esvm_streams (stream_id, camera_id, status)
        VALUES (%s, %s, 'inactive')
        ON CONFLICT (stream_id) DO UPDATE SET
            status = 'inactive';
        """
        cursor.execute(update_query, (camera_id, camera_id))
        conn.commit()
        print(f"Camera {camera_id} marked as inactive.")
    except Exception as e:
        print(f"Failed to mark camera {camera_id} as inactive:", str(e))
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Function to insert or update stream data in the database
def upsert_stream_data_into_db(stream_data, camera_id):
    if stream_data is None:
        print(f"No stream data for camera_id {camera_id}")
        return

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Extract info fields with default values if None
        info = stream_data.get('info', {})
        audio_codec = info.get('audio_codec', 'unknown')
        audio_sample_rate = info.get('audio_sample_rate', 0)
        total_bit_rate = info.get('total_bit_rate', 0)
        video_fps = info.get('video_fps', 0.0)
        frame_width = info.get('frame_width', 0)
        frame_height = info.get('frame_height', 0)
        video_codec = info.get('video_codec', 'unknown')
        transport_type = info.get('transport_type', 'unknown')

        # Upsert the stream data into the esvm_streams table
        upsert_query = """
        INSERT INTO esvm_streams (
            stream_id,
            camera_id,
            streamserver_id,
            streamserver_host,
            status,
            ts,
            audio_codec,
            audio_sample_rate,
            total_bit_rate,
            video_fps,
            frame_width,
            frame_height,
            video_codec,
            transport_type,
            live_url,
            info
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (stream_id) DO UPDATE SET
            streamserver_id = EXCLUDED.streamserver_id,
            streamserver_host = EXCLUDED.streamserver_host,
            status = EXCLUDED.status,
            ts = EXCLUDED.ts,
            audio_codec = EXCLUDED.audio_codec,
            audio_sample_rate = EXCLUDED.audio_sample_rate,
            total_bit_rate = EXCLUDED.total_bit_rate,
            video_fps = EXCLUDED.video_fps,
            frame_width = EXCLUDED.frame_width,
            frame_height = EXCLUDED.frame_height,
            video_codec = EXCLUDED.video_codec,
            transport_type = EXCLUDED.transport_type,
            live_url = EXCLUDED.live_url,
            info = EXCLUDED.info
        """
        cursor.execute(upsert_query, (
            stream_data.get('streamserver_id', 'unknown'),
            camera_id,
            stream_data.get('streamserver_id', 'unknown'),
            stream_data.get('streamserver_host', 'unknown'),
            stream_data.get('status', 'unknown'),
            stream_data.get('ts', 0),
            audio_codec,
            audio_sample_rate,
            total_bit_rate,
            video_fps,
            frame_width,
            frame_height,
            video_codec,
            transport_type,
            stream_data.get('live_url', 'unknown'),
            Json(info)
        ))

        conn.commit()
        print(f"Stream data upserted successfully for camera_id {camera_id}.")
    except Exception as e:
        print(f"Failed to upsert stream data into the database for camera_id {camera_id}:", str(e))
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Function to fetch all camera IDs from the database
def fetch_all_camera_ids():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute("SELECT camera_id FROM esvm_cameras")
        camera_ids = cursor.fetchall()
        return [camera_id[0] for camera_id in camera_ids]
    except Exception as e:
        print("Failed to fetch camera IDs:", str(e))
        return []
    finally:
        cursor.close()
        conn.close()

# Function to refresh token
def refresh_token(url, login, password):
    while True:
        token = get_token(url, login, password)
        if token:
            print("New token obtained:", token)
            yield token
            time.sleep(30 * 60)  # Sleep for 30 minutes before refreshing the token again
        else:
            print("Token refresh failed. Retrying in 5 seconds...")
            time.sleep(5)

# URL and credentials to obtain initial token
token_url = "https://esvm.kz/api/v1/token"
login = "cra_api@esvm.kz"
password = "qyKoZ7wosJf2W7AhOFINz5clCyOdKtD0"

# Obtain token
token_generator = refresh_token(token_url, login, password)
token = next(token_generator)

# Check if token obtained successfully
if token:
    # Fetch all camera IDs
    camera_ids = fetch_all_camera_ids()

    if camera_ids:
        for camera_id in tqdm(camera_ids, desc="Processing cameras"):
            # Construct the stream URL for each camera_id
            stream_url = f"https://esvm.kz/api/v1/streams/{camera_id}"
            stream_data = get_stream_data(stream_url, token)

            # Check if stream data obtained successfully
            if stream_data is not None:
                upsert_stream_data_into_db(stream_data, camera_id)
            else:
                mark_camera_as_inactive(camera_id)
else:
    print("Token not obtained. Cannot retrieve stream data.")
