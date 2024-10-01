import psycopg2
import requests
import time
import json
from retrying import retry
from tqdm import tqdm

# Database connection parameters
DB_HOST = '10.100.200.150'
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
        CREATE TABLE IF NOT EXISTS esvm_places_2 (
            id SERIAL PRIMARY KEY,
            place_id INTEGER,
            name TEXT,
            description TEXT,
            latitude NUMERIC,
            longitude NUMERIC,
            cohort_tracking BOOLEAN,
            group_id BIGINT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            plan_count INTEGER,
            camera_count INTEGER,
            category_id BIGINT,
            kind_id BIGINT,
            tags_id BIGINT[]
        )
        """

        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'esvm_places_2' created successfully or already exists.")
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
        INSERT INTO esvm_places_2 (place_id, name, description, latitude, longitude, cohort_tracking, group_id, created_at, updated_at, plan_count, camera_count, category_id, kind_id, tags_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        place_id = place_data.get("id")
        name = place_data.get("name")
        description = place_data.get("description")  # Description may be None
        latitude = place_data.get("latitude")
        longitude = place_data.get("longitude")
        cohort_tracking = place_data.get("cohort_tracking")
        group_id = place_data.get("group_id")
        created_at = place_data.get("created_at")
        updated_at = place_data.get("updated_at")
        plan_count = place_data.get("plan_count")
        camera_count = place_data.get("camera_count")
        category = place_data.get("category", {})
        category_id = category.get("id")
        kind = place_data.get("kind", {})
        kind_id = kind.get("id")
        tags = place_data.get("tags", [])
        tags_id = [tag.get("id") for tag in tags]

        # Replace None values with NULL for insertion into the database
        values = [place_id, name, description, latitude, longitude, cohort_tracking, group_id, created_at, updated_at, plan_count, camera_count, category_id, kind_id, tags_id]
        for i, value in enumerate(values):
            if value is None:
                values[i] = psycopg2.extensions.AsIs('NULL')

        cursor.execute(insert_query, values)
        conn.commit()
        # No need to print for each record insertion
    except psycopg2.IntegrityError as e:
        # Ignore duplicates
        conn.rollback()
    except Exception as e:
        print("Failed to insert data into the database:", str(e))
        print("Problematic data:", place_data)  # Print problematic data for debugging
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

        cursor.execute("SELECT place_id FROM esvm_places_2")
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

                for place in tqdm(places_data, desc="Inserting Places Data"):
                    place_id = place.get("id")
                    if place_id not in existing_place_ids:
                        insert_places_data_into_db(place)
                        existing_place_ids.add(place_id)

                if not cursor:
                    break
        except Exception as e:
            print("Failed to retrieve or insert data:", str(e))
        finally:
            time.sleep(10)  # Wait for 5 minutes before restarting the loop

if __name__ == "__main__":
    main()
