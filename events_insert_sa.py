import asyncio
import requests
import json
import psycopg2
from datetime import datetime, timedelta
from tqdm import tqdm  # for progress bar

# Database connection parameters
DB_HOST = '10.100.200.102'
DB_PORT = '5439'
DB_NAME = 'sitcenter_postgis_datalake'
DB_USER = 'la_noche_estrellada'
DB_PASSWORD = 'Cfq,thNb13@'

# Function to obtain token
async def get_token(url, login, password):
    payload = {
        "grant_type": "password",
        "username": login,
        "password": password
    }
    while True:
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            data = response.json()
            return data.get("access_token")
        else:
            print("Failed to obtain token. Status code:", response.status_code)
            await asyncio.sleep(5)  # Wait for 5 seconds before retrying

# Function to get events data with pagination support
async def get_events_data_with_pagination(url, token, ts_from, ts_to, event_type, cursor=None, limit=20, max_retries=3):
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "ts_from": ts_from,
        "ts_to": ts_to,
        "event_type": event_type,
        "cursor": cursor,
        "limit": limit
    }
    retries = 0
    while retries < max_retries:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 503:
            print("Service Unavailable (503). Retrying after 5 seconds...")
            retries += 1
            await asyncio.sleep(5)  # Wait for 5 seconds before retrying
        elif response.status_code == 401:
            print("Unauthorized (401). Retrying after 5 seconds...")
            retries += 1
            await asyncio.sleep(5)  # Wait for 5 seconds before retrying
        else:
            print("Failed to get events data. Status code:", response.status_code)
            print("Response content:", response.content)
            return None
    print("Max retries reached. Unable to fetch events data.")
    return None


# Function to connect to the PostgreSQL database
def connect_to_database():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except psycopg2.Error as e:
        print("Error connecting to the database:", e)
        return None

# Function to insert or update events data into the database
async def insert_or_update_events_data(event_data, conn):
    try:
        cursor = conn.cursor()

        # Extract event attributes or set them to None if not available
        event_attributes = event_data["event"].get("roi")
        if event_attributes is None:
            name = None

        else:
            name = event_attributes.get("name")

        # Check if the event exists
        cursor.execute("SELECT COUNT(*) FROM esvm_events_sa WHERE event_id = %s", (event_data["event_id"],))
        count = cursor.fetchone()[0]
        if count > 0:
            # Update the existing event
            update_event_query = """
                UPDATE esvm_events_sa
                SET camera_id = %s, camera_name = %s, place_id = %s, place_name = %s,
                    place_latitude = %s, place_longitude = %s, place_description = %s,
                    event_type = %s, event_ts = %s, event_reason = %s, event_label = %s,
                    event_detection = %s, event_attributes = %s, event_roi = %s,
                    snapshot_url = %s, streamserver_id = %s, stream_id = %s, obsolete = %s,
                    roi_name = %s
                WHERE event_id = %s
            """
            cursor.execute(update_event_query, (
                event_data["origin"]["camera_id"], event_data["origin"]["camera_name"],
                event_data["origin"]["place_id"], event_data["origin"]["place_name"],
                event_data["origin"]["place_latitude"], event_data["origin"]["place_longitude"],
                event_data["origin"]["place_description"], event_data["event_type"],
                datetime.fromtimestamp(event_data["ts"] / 1000), event_data["event"]["reason"],
                event_data["event"]["label"], json.dumps(event_data["event"]["detection"]),
                json.dumps(event_attributes), json.dumps(event_data["event"]["roi"]),
                event_data["snapshot_url"], event_data["streamserver_id"], event_data["stream_id"],
                event_data["obsolete"],
                name,
                event_data["event_id"]
            ))
        else:
            # Insert the new event
            insert_event_query = """
                INSERT INTO esvm_events_sa (
                    event_id, camera_id, camera_name, place_id, place_name,
                    place_latitude, place_longitude, place_description, event_type,
                    event_ts, event_reason, event_label, event_detection,
                    event_attributes, event_roi, snapshot_url,
                    streamserver_id, stream_id, obsolete,
                    roi_name
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s
                )
            """
            cursor.execute(insert_event_query, (
                event_data["event_id"], event_data["origin"]["camera_id"], event_data["origin"]["camera_name"],
                event_data["origin"]["place_id"], event_data["origin"]["place_name"],
                event_data["origin"]["place_latitude"], event_data["origin"]["place_longitude"],
                event_data["origin"]["place_description"], event_data["event_type"],
                datetime.fromtimestamp(event_data["ts"] / 1000), event_data["event"]["reason"],
                event_data["event"]["label"], json.dumps(event_data["event"]["detection"]),
                json.dumps(event_attributes), json.dumps(event_data["event"]["roi"]),
                event_data["snapshot_url"], event_data["streamserver_id"], event_data["stream_id"],
                event_data["obsolete"],
                name
            ))
        conn.commit()
        print("Event data inserted or updated into the database.")
    except psycopg2.Error as e:
        print("Failed to insert or update event data into the database:", e)
        conn.rollback()
    finally:
        if cursor:
            cursor.close()


async def process_events_for_type(event_type, token, ts_from, ts_to, events_url, conn):
    print(f"Fetching events for type: {event_type}")
    cursor = None  # Reset cursor for each event type
    while True:
        # Get events data with pagination
        print(f"Fetching events data for type: {event_type}, ts_from: {ts_from}, ts_to: {ts_to}, cursor: {cursor}")
        events_data = await get_events_data_with_pagination(events_url, token, ts_from, ts_to, event_type, cursor)

        # Check if events data obtained successfully
        if events_data:
            # Insert or update data into the database
            for event_data in tqdm(events_data["events"], desc=f"Processing events for {event_type}"):
                await insert_or_update_events_data(event_data, conn)

            # Check for pagination
            if "pagination" in events_data and "cursor" in events_data["pagination"]:
                cursor = events_data["pagination"]["cursor"]
            else:
                break  # No more pages available
        else:
            print(f"Unable to fetch events data for type: {event_type}")
            break  # Stop fetching if unable to get data

async def main():
    # URL and credentials to obtain initial token
    token_url = "https://esvm.kz/api/v1/token"
    login = "cra_api@esvm.kz"
    password = "qyKoZ7wosJf2W7AhOFINz5clCyOdKtD0"

    # Define the events URL
    events_url = "https://esvm.kz/api/v1/events"

    while True:
        # Obtain token with retry logic
        token = None
        while not token:
            print("Attempting to obtain token...")
            token = await get_token(token_url, login, password)

        # Define time range for events (last 5 hours)
        end_time = datetime.now()  # End time, current time
        start_time = end_time - timedelta(hours=5)  # Start time, 5 hours ago

        # Format timestamps in ISO 8601 format
        ts_from = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        ts_to = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Connect to the database
        conn = connect_to_database()
        if conn:
            # List of event types
            event_types = ["sa",]

            # Fetch events for each event type concurrently
            await asyncio.gather(*[process_events_for_type(event_type, token, ts_from, ts_to, events_url, conn) for event_type in event_types])

            # Close the database connection
            conn.close()
        else:
            print("Failed to connect to the database.")

        # Wait for 5 minutes before fetching data again
        await asyncio.sleep(300)

# Run the main coroutine
asyncio.run(main())
