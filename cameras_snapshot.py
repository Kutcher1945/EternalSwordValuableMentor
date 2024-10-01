import os
import requests
import psycopg2
import json
import time
import logging
import threading
from retrying import retry
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# Database connection parameters
DB_HOST = '10.100.200.150'
DB_PORT = '5439'
DB_NAME = 'sitcenter_postgis_datalake'
DB_USER = 'la_noche_estrellada'
DB_PASSWORD = 'Cfq,thNb13@'

# Directory to save images
IMAGE_DIR = 'camera_snapshots'

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Ensure the image directory exists
os.makedirs(IMAGE_DIR, exist_ok=True)

# Function to create table if not exists
def create_table_if_not_exists():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS esvm_cameras (
            camera_id TEXT UNIQUE,
            name TEXT,
            source_url TEXT,
            video_analytics JSONB,
            video_streaming_by_event BOOLEAN,
            video_streaming_storage_depth INTEGER,
            place_id INTEGER,
            onvif_host TEXT,
            onvif_port INTEGER,
            onvif_login TEXT,
            onvif_password TEXT,
            scheduler_group TEXT,
            state TEXT,
            state_reason TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            snapshot_url TEXT,
            place_name TEXT,
            place_description TEXT,
            place_latitude NUMERIC,
            place_longitude NUMERIC,
            capacitygroup_token TEXT,
            plan_id TEXT,
            plan_x NUMERIC,
            plan_y NUMERIC,
            ptz JSONB,
            scheduler_tags JSONB,
            geom GEOMETRY,
            is_deleted BOOLEAN DEFAULT FALSE,
            is_ao BOOLEAN DEFAULT FALSE,
            is_fr BOOLEAN DEFAULT FALSE,
            is_lpr BOOLEAN DEFAULT FALSE,
            is_sa BOOLEAN DEFAULT FALSE,
            is_tp BOOLEAN DEFAULT FALSE
        )
        """

        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Table 'esvm_cameras' created successfully or already exists.")
    except Exception as e:
        logger.error(f"Failed to create table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Function to extract module information and set boolean fields
def extract_module_info(video_analytics):
    if video_analytics is None:
        return {
            "is_ao": False,
            "is_fr": False,
            "is_lpr": False,
            "is_sa": False,
            "is_tp": False
        }
    modules = video_analytics.get("modules", [])
    module_info = {
        "is_ao": False,
        "is_fr": False,
        "is_lpr": False,
        "is_sa": False,
        "is_tp": False
    }
    for module in modules:
        module_name = module.get("module")
        if module_name == "AbandonedObjectsModule":
            module_info["is_ao"] = True
        elif module_name == "FaceRecognitionModule":
            module_info["is_fr"] = True
        elif module_name == "LicensePlateRecognitionModule":
            module_info["is_lpr"] = True
        elif module_name == "SituationAnalyticsModule":
            module_info["is_sa"] = True
        elif module_name == "TamperingModule":
            module_info["is_tp"] = True
    return module_info

# Function to insert or update data in the database
def insert_data_into_db(data):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        video_analytics = data.get("video_analytics", {})
        module_info = extract_module_info(video_analytics)

        # Check if the camera already exists in the database
        select_query = "SELECT camera_id FROM esvm_cameras WHERE camera_id = %s"
        cursor.execute(select_query, (data["id"],))
        existing_record = cursor.fetchone()

        if existing_record:
            # Update the existing record
            update_query = """
            UPDATE esvm_cameras
            SET name = %s,
                source_url = %s,
                video_analytics = %s,
                video_streaming_by_event = %s,
                video_streaming_storage_depth = %s,
                place_id = %s,
                onvif_host = %s,
                onvif_port = %s,
                onvif_login = %s,
                onvif_password = %s,
                scheduler_group = %s,
                state = %s,
                state_reason = %s,
                created_at = %s,
                updated_at = %s,
                snapshot_url = %s,
                place_name = %s,
                place_description = %s,
                place_latitude = %s,
                place_longitude = %s,
                capacitygroup_token = %s,
                plan_id = %s,
                plan_x = %s,
                plan_y = %s,
                ptz = %s,
                scheduler_tags = %s,
                is_deleted = FALSE,
                is_ao = %s,
                is_fr = %s,
                is_lpr = %s,
                is_sa = %s,
                is_tp = %s
            WHERE camera_id = %s
            """
            cursor.execute(update_query, (
                data.get("name"),
                data.get("source", {}).get("url"),
                json.dumps(data.get("video_analytics")),
                data.get("video_streaming", {}).get("by_event"),
                data.get("video_streaming", {}).get("storage_depth"),
                data.get("place_id"),
                data.get("onvif_host"),
                data.get("onvif_port"),
                data.get("onvif_login"),
                data.get("onvif_password"),
                data.get("scheduler_group"),
                data.get("state"),
                data.get("state_reason"),
                data.get("created_at"),
                data.get("updated_at"),
                data.get("snapshot_url"),
                data.get("origin", {}).get("place_name"),
                data.get("origin", {}).get("place_description"),
                data.get("origin", {}).get("place_latitude"),
                data.get("origin", {}).get("place_longitude"),
                data.get("capacitygroup_token"),
                data.get("plan_id"),
                data.get("plan_x"),
                data.get("plan_y"),
                json.dumps(data.get("ptz")),
                json.dumps(data.get("scheduler_tags")),
                module_info["is_ao"],
                module_info["is_fr"],
                module_info["is_lpr"],
                module_info["is_sa"],
                module_info["is_tp"],
                data["id"]
            ))
        else:
            # Insert a new record
            insert_query = """
            INSERT INTO esvm_cameras (
                camera_id, 
                name, 
                source_url, 
                video_analytics, 
                video_streaming_by_event, 
                video_streaming_storage_depth, 
                place_id, 
                onvif_host, 
                onvif_port, 
                onvif_login, 
                onvif_password, 
                scheduler_group, 
                state, 
                state_reason, 
                created_at, 
                updated_at, 
                snapshot_url, 
                place_name, 
                place_description, 
                place_latitude, 
                place_longitude,
                capacitygroup_token,
                plan_id,
                plan_x,
                plan_y,
                ptz,
                scheduler_tags,
                geom,
                is_deleted,
                is_ao,
                is_fr,
                is_lpr,
                is_sa,
                is_tp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), FALSE, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                data.get("id"),  # Using ID as camera_id
                data.get("name"),
                data.get("source", {}).get("url"),
                json.dumps(data.get("video_analytics")),
                data.get("video_streaming", {}).get("by_event"),
                data.get("video_streaming", {}).get("storage_depth"),
                data.get("place_id"),
                data.get("onvif_host"),
                data.get("onvif_port"),
                data.get("onvif_login"),
                data.get("onvif_password"),
                data.get("scheduler_group"),
                data.get("state"),
                data.get("state_reason"),
                data.get("created_at"),
                data.get("updated_at"),
                data.get("snapshot_url"),
                data.get("origin", {}).get("place_name"),
                data.get("origin", {}).get("place_description"),
                data.get("origin", {}).get("place_latitude"),
                data.get("origin", {}).get("place_longitude"),
                data.get("capacitygroup_token"),
                data.get("plan_id"),
                data.get("plan_x"),
                data.get("plan_y"),
                json.dumps(data.get("ptz")),
                json.dumps(data.get("scheduler_tags")),
                data.get("origin", {}).get("place_longitude"),
                data.get("origin", {}).get("place_latitude"),
                module_info["is_ao"],
                module_info["is_fr"],
                module_info["is_lpr"],
                module_info["is_sa"],
                module_info["is_tp"]
            ))
            logger.info(f"New camera ID inserted: {data.get('id')}")

        conn.commit()

        # Check if geom is NULL and set is_deleted to TRUE if it is
        set_is_deleted_if_geom_null(cursor, data["id"])

        # Download the image
        download_image(data.get("snapshot_url"), data.get("id"))

    except psycopg2.IntegrityError as e:
        logger.error(f"IntegrityError while inserting/updating data: {str(e)}")
        conn.rollback()
    except Exception as e:
        logger.error(f"Failed to insert/update data into the database: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def set_is_deleted_if_geom_null(cursor, camera_id):
    update_query = """
    UPDATE esvm_cameras
    SET is_deleted = TRUE
    WHERE camera_id = %s AND geom IS NULL
    RETURNING camera_id
    """
    cursor.execute(update_query, (camera_id,))
    deleted_camera_id = cursor.fetchone()
    if deleted_camera_id:
        logger.info(f"Camera ID {deleted_camera_id[0]} set to deleted due to NULL geom.")

def download_image(url, camera_id):
    if not url:
        logger.warning(f"No snapshot URL provided for camera ID {camera_id}. Skipping download.")
        return

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        image_path = os.path.join(IMAGE_DIR, f"{camera_id}.jpg")
        with open(image_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Downloaded snapshot for camera ID {camera_id}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download snapshot for camera ID {camera_id}: {str(e)}")

@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
def get_cameras_data_with_retry(url, token, cursor=None):
    headers = {"Authorization": f"Bearer {token}"}
    params = {"cursor": cursor} if cursor else None
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

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
        logger.error(f"Failed to obtain token. Status code: {response.status_code}, Response: {response.text}")
        return None

# Function to refresh token
def refresh_token(url, login, password):
    while True:
        token = get_token(url, login, password)
        if token:
            logger.info(f"New token obtained: {token}")
            yield token
            time.sleep(1740)  # Sleep for 29 minutes (29 * 60 = 1740 seconds)
        else:
            logger.error("Token refresh failed. Retrying in 5 seconds...")
            time.sleep(5)

# Periodically refresh token in a separate thread
def refresh_token_periodically():
    global access_token, token_generator
    while True:
        access_token = next(token_generator)
        if not access_token:
            logger.error("Failed to refresh token. Re-authenticating...")
            access_token = get_token(token_url, login, password)
            if not access_token:
                logger.error("Failed to re-authenticate. Exiting...")
                raise RuntimeError("Failed to re-authenticate. Exiting...")
        else:
            logger.info("Token refreshed successfully.")

# Function to process each camera in a thread
def process_camera(camera, token):
    if camera is not None:
        insert_data_into_db(camera)
        return camera["id"]
    else:
        logger.error("Received None camera data to process.")
        return None

# Function to mark camera as deleted if not in the API
def mark_camera_as_deleted(camera_id):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        update_query = "UPDATE esvm_cameras SET is_deleted = TRUE WHERE camera_id = %s RETURNING camera_id"
        cursor.execute(update_query, (camera_id,))

        conn.commit()
        deleted_camera_id = cursor.fetchone()
        if deleted_camera_id:
            logger.info(f"Camera ID marked as deleted: {deleted_camera_id[0]}")
    except Exception as e:
        logger.error(f"Failed to mark camera as deleted: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# URL and credentials to obtain initial token
token_url = "https://esvm.kz/api/v1/token"
login = "cra_api@esvm.kz"
password = "qyKoZ7wosJf2W7AhOFINz5clCyOdKtD0"

# URL to get cameras data
cameras_url = "https://esvm.kz/api/v1/cameras"

# Create table if not exists
create_table_if_not_exists()

# Generator to refresh token only if it's about to expire or has expired
token_generator = refresh_token(token_url, login, password)

# Obtain initial token
access_token = next(token_generator)

# Check if token obtained successfully
if access_token:
    # Start the token refresh thread
    token_refresh_thread = threading.Thread(target=refresh_token_periodically, daemon=True)
    token_refresh_thread.start()

    while True:
        try:
            # Start fetching cameras data
            cursor = None
            existing_camera_ids = set()  # Keep track of camera_ids fetched from the API
            while True:
                cameras_data = get_cameras_data_with_retry(cameras_url, access_token, cursor)
                if cameras_data:
                    total_cameras = len(cameras_data["cameras"])
                    logger.info(f"Total cameras to insert/update: {total_cameras}")
                    # Process each camera data using threading
                    with ThreadPoolExecutor(max_workers=10) as executor:
                        futures = [executor.submit(process_camera, camera, access_token) for camera in cameras_data["cameras"]]
                        for future in tqdm(as_completed(futures), total=total_cameras, desc="Processing cameras"):
                            try:
                                camera_id = future.result()
                                if camera_id:
                                    existing_camera_ids.add(camera_id)
                            except Exception as e:
                                logger.error(f"Error processing camera: {e}")

                    if "pagination" in cameras_data and "cursor" in cameras_data["pagination"]:
                        cursor = cameras_data["pagination"]["cursor"]
                    else:
                        break
                else:
                    break

            # Mark cameras as deleted if they are not found in the latest API response
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            cursor = conn.cursor()
            cursor.execute("SELECT camera_id FROM esvm_cameras WHERE is_deleted = FALSE")
            all_camera_ids = {row[0] for row in cursor.fetchall()}
            for camera_id in all_camera_ids - existing_camera_ids:
                mark_camera_as_deleted(camera_id)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.warning("Received 401 Unauthorized error. Attempting to obtain a new token.")
                access_token = next(token_generator)
                if access_token:
                    logger.info(f"New token obtained successfully: {access_token}")
                else:
                    logger.error("Failed to obtain a new token. Exiting.")
                    break
            else:
                logger.error(f"Failed to get cameras data. Status code: {e.response.status_code}")
                break
        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            break
else:
    logger.error("Token not obtained. Cannot retrieve cameras data.")
