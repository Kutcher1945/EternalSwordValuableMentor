import requests
import psycopg2
from psycopg2.extras import Json
import time
from tqdm import tqdm
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Database connection parameters
DB_HOST = '10.100.200.102'
DB_PORT = '5439'
DB_NAME = 'sitcenter_postgis_datalake'
DB_USER = 'la_noche_estrellada'
DB_PASSWORD = 'Cfq,thNb13@'

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# URL and credentials to obtain initial token
token_url = "https://esvm.kz/api/v1/token"
refresh_token_url = "https://esvm.kz/api/v1/token/refresh"
login = "cra_api@esvm.kz"
password = "qyKoZ7wosJf2W7AhOFINz5clCyOdKtD0"

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
        return data.get("access_token"), data.get("refresh_token")
    else:
        logger.error(f"Failed to obtain token. Status code: {response.status_code}, Response: {response.text}")
        return None, None

# Function to refresh token
def refresh_token(url, refresh_token):
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }
    response = requests.post(url, data=payload)
    if response.status_code == 200:
        data = response.json()
        logger.info("Token refreshed successfully.")
        return data.get("access_token"), data.get("refresh_token")
    else:
        logger.error(f"Failed to refresh token. Status code: {response.status_code}, Response: {response.text}")
        return None, None

# Function to periodically refresh the token
def refresh_token_periodically():
    global access_token, refresh_token_value
    while True:
        time.sleep(1740)  # Sleep for 29 minutes (29 * 60 = 1740 seconds)
        access_token, refresh_token_value = refresh_token(refresh_token_url, refresh_token_value)
        if not access_token:
            logger.error("Failed to refresh token. Re-authenticating...")
            access_token, refresh_token_value = get_token(token_url, login, password)
            if not access_token:
                logger.error("Failed to re-authenticate. Exiting...")
                raise RuntimeError("Failed to re-authenticate. Exiting...")
        else:
            logger.info("Token refreshed successfully.")

# Function to get stream data with retry logic
def get_stream_data(url, token, retries=5, backoff_factor=1):
    headers = {"Authorization": f"Bearer {token}"}
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                raise PermissionError("Token expired")
            elif response.status_code == 404:
                return None  # Indicate that the stream was not found
            else:
                logger.error(f"Failed to get stream data. Status code: {response.status_code}. Response content: {response.content}")
                return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed: {e}. Retrying in {backoff_factor * (attempt + 1)} seconds...")
            time.sleep(backoff_factor * (attempt + 1))
    logger.error(f"Failed to get stream data after {retries} attempts.")
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
        INSERT INTO esvm_streams (camera_id, status, created_at, updated_at)
        VALUES (%s, 'inactive', NOW(), NOW())
        ON CONFLICT (camera_id) DO UPDATE SET
            status = 'inactive',
            updated_at = NOW();
        """
        cursor.execute(update_query, (camera_id,))
        conn.commit()
        logger.info(f"Camera {camera_id} marked as inactive.")
    except Exception as e:
        logger.error(f"Failed to mark camera {camera_id} as inactive: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Function to insert data into the historical table
def insert_into_historical_table(stream_data, camera_id):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Ensure default values for fields that might be None
        info = stream_data.get('info') or {}
        audio_codec = info.get('audio_codec') or 'unknown'
        audio_sample_rate = info.get('audio_sample_rate') or 0
        total_bit_rate = info.get('total_bit_rate') or 0
        video_fps = info.get('video_fps') or 0.0
        frame_width = info.get('frame_width') or 0
        frame_height = info.get('frame_height') or 0
        video_codec = info.get('video_codec') or 'unknown'
        transport_type = info.get('transport_type') or 'unknown'

        insert_query = """
        INSERT INTO esvm_streams_historical (
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
            info,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW());
        """
        cursor.execute(insert_query, (
            stream_data.get('streamserver_id') or 'unknown',
            camera_id,
            stream_data.get('streamserver_id') or 'unknown',
            stream_data.get('streamserver_host') or 'unknown',
            stream_data.get('status') or 'unknown',
            stream_data.get('ts') or 0,
            audio_codec,
            audio_sample_rate,
            total_bit_rate,
            video_fps,
            frame_width,
            frame_height,
            video_codec,
            transport_type,
            stream_data.get('live_url') or 'unknown',
            Json(info)
        ))

        conn.commit()
        logger.info(f"Stream data inserted into historical table for camera_id {camera_id}.")
    except Exception as e:
        logger.error(f"Failed to insert stream data into the historical table for camera_id {camera_id}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Function to insert or update stream data in the database
def upsert_stream_data_into_db(stream_data, camera_id):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Ensure default values for fields that might be None
        info = stream_data.get('info') or {}
        audio_codec = info.get('audio_codec') or 'unknown'
        audio_sample_rate = info.get('audio_sample_rate') or 0
        total_bit_rate = info.get('total_bit_rate') or 0
        video_fps = info.get('video_fps') or 0.0
        frame_width = info.get('frame_width') or 0
        frame_height = info.get('frame_height') or 0
        video_codec = info.get('video_codec') or 'unknown'
        transport_type = info.get('transport_type') or 'unknown'

        # Unique stream identifier
        stream_id = f"{stream_data.get('streamserver_id')}_{camera_id}"

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
            info,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (camera_id) DO UPDATE SET
            stream_id = EXCLUDED.stream_id,
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
            info = EXCLUDED.info,
            updated_at = NOW();
        """
        cursor.execute(upsert_query, (
            stream_id,
            camera_id,
            stream_data.get('streamserver_id') or 'unknown',
            stream_data.get('streamserver_host') or 'unknown',
            stream_data.get('status') or 'unknown',
            stream_data.get('ts') or 0,
            audio_codec,
            audio_sample_rate,
            total_bit_rate,
            video_fps,
            frame_width,
            frame_height,
            video_codec,
            transport_type,
            stream_data.get('live_url') or 'unknown',
            Json(info)
        ))

        conn.commit()
        logger.info(f"Stream data upserted successfully for camera_id {camera_id}.")
        insert_into_historical_table(stream_data, camera_id)
    except Exception as e:
        logger.error(f"Failed to upsert stream data into the database for camera_id {camera_id}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Function to fetch distinct camera IDs from the database
def fetch_distinct_camera_ids():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT camera_id FROM esvm_cameras")
        camera_ids = cursor.fetchall()
        return [camera_id[0] for camera_id in camera_ids]
    except Exception as e:
        logger.error("Failed to fetch camera IDs: " + str(e))
        return []
    finally:
        cursor.close()
        conn.close()

# Function to fetch existing camera IDs from the esvm_streams table
def fetch_existing_camera_ids():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT camera_id FROM esvm_streams")
        existing_camera_ids = cursor.fetchall()
        return {camera_id[0] for camera_id in existing_camera_ids}
    except Exception as e:
        logger.error("Failed to fetch existing camera IDs: " + str(e))
        return set()
    finally:
        cursor.close()
        conn.close()

# Obtain initial token
access_token, refresh_token_value = get_token(token_url, login, password)

# Keep track of processed camera IDs
processed_camera_ids = set()

def process_camera(camera_id):
    global access_token, refresh_token_value, processed_camera_ids
    try:
        # Construct the stream URL for each camera_id
        stream_url = f"https://esvm.kz/api/v1/streams/{camera_id}"
        stream_data = get_stream_data(stream_url, access_token)

        # Check if stream data obtained successfully
        if stream_data is not None:
            # logger.info(f"Stream data for camera_id {camera_id}: {stream_data}")
            upsert_stream_data_into_db(stream_data, camera_id)
        else:
            mark_camera_as_inactive(camera_id)
        processed_camera_ids.add(camera_id)
    except PermissionError:
        logger.warning("Token expired. Refreshing token...")
        access_token, refresh_token_value = refresh_token(refresh_token_url, refresh_token_value)
        if not access_token:
            logger.error("Failed to refresh token. Re-authenticating...")
            access_token, refresh_token_value = get_token(token_url, login, password)
            if not access_token:
                logger.error("Failed to re-authenticate. Exiting...")
                raise RuntimeError("Failed to re-authenticate. Exiting...")
        process_camera(camera_id)  # Retry after refreshing the token
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}. Retrying in 10 seconds...")
        time.sleep(10)
        process_camera(camera_id)  # Retry after a delay

# Function to start the processing loop
def start_processing_loop():
    global access_token, refresh_token_value, processed_camera_ids
    while True:
        # Fetch distinct camera IDs
        camera_ids = fetch_distinct_camera_ids()
        if not camera_ids:
            logger.error("No camera IDs found. Exiting...")
            break

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(process_camera, camera_id) for camera_id in camera_ids]
            for future in tqdm(as_completed(futures), total=len(camera_ids), desc="Processing cameras"):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing camera: {e}")

        logger.info("Finished processing all camera IDs. Restarting...")

# Check if tokens obtained successfully
if access_token and refresh_token_value:
    # Start the token refresh thread
    token_refresh_thread = threading.Thread(target=refresh_token_periodically, daemon=True)
    token_refresh_thread.start()

    # Start the processing loop
    start_processing_loop()
else:
    logger.error("Token not obtained. Cannot retrieve stream data.")
