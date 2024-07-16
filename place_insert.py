import psycopg2
import requests
import time
from retrying import retry
from tqdm import tqdm
import json

# Database connection parameters
DB_HOST = '172.30.227.205'
DB_PORT = '5439'
DB_NAME = 'sitcenter_postgis_datalake'
DB_USER = 'la_noche_estrellada'
DB_PASSWORD = 'Cfq,thNb13@'

def create_places_table_if_not_exists():
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
        CREATE TABLE IF NOT EXISTS esvm_places (
            place_id        INTEGER PRIMARY KEY,
            name            TEXT,
            description     TEXT,
            latitude        NUMERIC(9, 6),
            longitude       NUMERIC(9, 6),
            camera_count    INTEGER,
            category        JSONB,
            cohort_tracking BOOLEAN NOT NULL,
            created_at      TIMESTAMP WITH TIME ZONE,
            group_id        INTEGER,
            kind            TEXT,
            plan_count      INTEGER,
            tags            JSONB,
            updated_at      TIMESTAMP WITH TIME ZONE
        )
        """

        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'esvm_places' created successfully or already exists.")
    except Exception as e:
        print("Failed to create table:", str(e))
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def insert_places_data_into_db(place_data):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO esvm_places (
            place_id, name, description, latitude, longitude, 
            camera_count, category, cohort_tracking, created_at, 
            group_id, kind, plan_count, tags, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        update_query = """
        UPDATE esvm_places
        SET name = %s,
            description = %s,
            latitude = %s,
            longitude = %s,
            camera_count = %s,
            category = %s,
            cohort_tracking = %s,
            created_at = %s,
            group_id = %s,
            kind = %s,
            plan_count = %s,
            tags = %s,
            updated_at = %s
        WHERE place_id = %s
        """

        place_id = place_data.get("id")
        name = place_data.get("name")
        description = place_data.get("description")
        latitude = place_data.get("latitude")
        longitude = place_data.get("longitude")
        camera_count = place_data.get("camera_count")
        category = json.dumps(place_data.get("category")) if place_data.get("category") else None
        cohort_tracking = place_data.get("cohort_tracking")
        created_at = place_data.get("created_at")
        group_id = place_data.get("group_id")
        kind = place_data.get("kind")
        plan_count = place_data.get("plan_count")
        tags = json.dumps(place_data.get("tags")) if place_data.get("tags") else None
        updated_at = place_data.get("updated_at")

        # Check if the place already exists in the database
        select_query = "SELECT place_id FROM esvm_places WHERE place_id = %s"
        cursor.execute(select_query, (place_id,))
        existing_record = cursor.fetchone()

        if existing_record:
            # Update the existing record
            cursor.execute(update_query, (
                name, description, latitude, longitude, 
                camera_count, category, cohort_tracking, created_at, 
                group_id, kind, plan_count, tags, updated_at,
                place_id
            ))
            print(f"Updated place with place_id: {place_id}")
        else:
            # Insert a new record
            cursor.execute(insert_query, (
                place_id, name, description, latitude, longitude, 
                camera_count, category, cohort_tracking, created_at, 
                group_id, kind, plan_count, tags, updated_at
            ))
            print(f"Inserted new place with place_id: {place_id}")

        conn.commit()
    except psycopg2.IntegrityError as e:
        print(f"IntegrityError while inserting/updating data: {str(e)}")
        conn.rollback()
    except Exception as e:
        print("Failed to insert/update data into the database:", str(e))
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
def get_places_data_with_retry(url, token, cursor=None):
    headers = {"Authorization": f"Bearer {token}"}
    params = {"cursor": cursor} if cursor else None
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    places_data = data.get("places")
    cursor = data.get("pagination", {}).get("cursor")
    return places_data, cursor

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

def refresh_token(url, login, password):
    while True:
        token = get_token(url, login, password)
        if token:
            print("New token obtained:", token)
            break
        time.sleep(5)
    return token

def get_existing_place_ids():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        cursor.execute("SELECT place_id FROM esvm_places")
        rows = cursor.fetchall()
        existing_place_ids = {row[0] for row in rows}  # Using set comprehension to store unique place IDs
        return existing_place_ids
    except Exception as e:
        print("Failed to get existing place IDs from the database:", str(e))
        return set()
    finally:
        cursor.close()
        conn.close()

# Main function to insert places data into the database
def main():
    token_url = "https://esvm.kz/api/v1/token"
    places_url = "https://esvm.kz/api/v1/places"
    login = "cra_api@esvm.kz"
    password = "qyKoZ7wosJf2W7AhOFINz5clCyOdKtD0"

    while True:
        token = refresh_token(token_url, login, password)

        create_places_table_if_not_exists()

        existing_place_ids = get_existing_place_ids()

        cursor = None
        try:
            while True:
                places_data, cursor = get_places_data_with_retry(places_url, token, cursor)
                if not places_data:
                    break

                for place in tqdm(places_data, desc="Inserting/Updating Places Data"):
                    place_id = place.get("id")
                    insert_places_data_into_db(place)
                    existing_place_ids.add(place_id)

                if not cursor:
                    break
        except Exception as e:
            print("Failed to retrieve or insert data:", str(e))
        finally:
            time.sleep(10)  # Wait for 10 seconds before restarting the loop

if __name__ == "__main__":
    main()
