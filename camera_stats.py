import requests
import psycopg2
from datetime import datetime, time
import time as tm

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

# Function to refresh token
def refresh_token(url, login, password):
    while True:
        token = get_token(url, login, password)
        if token:
            print("New token obtained:", token)
            yield token
            tm.sleep(30 * 60)  # Sleep for 30 minutes before refreshing the token again
        else:
            print("Token refresh failed. Retrying in 5 seconds...")
            tm.sleep(5)

# Function to create stats table if not exists
def create_stats_table_if_not_exists():
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
        CREATE TABLE IF NOT EXISTS esvm_camera_stats (
            id SERIAL PRIMARY KEY,
            total_count INTEGER,
            active_count INTEGER,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
        """

        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'esvm_camera_stats' created successfully or already exists.")
    except Exception as e:
        print("Failed to create stats table:", str(e))
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Function to create historical stats table if not exists
def create_historical_stats_table_if_not_exists():
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
        CREATE TABLE IF NOT EXISTS esvm_camera_stats_history (
            id SERIAL PRIMARY KEY,
            total_count INTEGER,
            active_count INTEGER,
            recorded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
        """

        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'esvm_camera_stats_history' created successfully or already exists.")
    except Exception as e:
        print("Failed to create historical stats table:", str(e))
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Function to get camera stats
def get_camera_stats(url, token):
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to get camera stats. Status code:", response.status_code)
        print("Response content:", response.content)
        return None

# Function to update camera stats in the database
def update_camera_stats_in_db(stats):
    updated = False
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
        UPDATE esvm_camera_stats
        SET total_count = %s, active_count = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = (SELECT id FROM esvm_camera_stats ORDER BY created_at DESC LIMIT 1)
        RETURNING id
        """
        cursor.execute(update_query, (
            stats.get('total_count', 0),
            stats.get('active_count', 0)
        ))

        if cursor.rowcount > 0:
            updated = True
            print("Camera stats updated successfully.")
        else:
            insert_query = """
            INSERT INTO esvm_camera_stats (total_count, active_count)
            VALUES (%s, %s)
            """
            cursor.execute(insert_query, (
                stats.get('total_count', 0),
                stats.get('active_count', 0)
            ))
            print("Camera stats inserted successfully.")

        conn.commit()
    except Exception as e:
        print("Failed to update/insert camera stats in the database:", str(e))
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
    return updated

# Function to insert historical camera stats into the database
def insert_camera_stats_into_history_db(stats):
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
        INSERT INTO esvm_camera_stats_history (total_count, active_count, recorded_at)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        """

        cursor.execute(insert_query, (
            stats.get('total_count', 0),
            stats.get('active_count', 0)
        ))

        conn.commit()
        print("Historical camera stats inserted successfully.")
    except Exception as e:
        print("Failed to insert historical camera stats into the database:", str(e))
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Function to fetch the latest stats from the database
def get_latest_stats_from_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        select_query = "SELECT total_count, active_count FROM esvm_camera_stats ORDER BY created_at DESC LIMIT 1"
        cursor.execute(select_query)
        result = cursor.fetchone()
        return result if result else (None, None)
    except Exception as e:
        print("Failed to fetch the latest stats from the database:", str(e))
        return (None, None)
    finally:
        cursor.close()
        conn.close()

# URL and credentials to obtain initial token
token_url = "https://esvm.kz/api/v1/token"
login = "cra_api@esvm.kz"
password = "qyKoZ7wosJf2W7AhOFINz5clCyOdKtD0"

# Obtain token
token_generator = refresh_token(token_url, login, password)
token = next(token_generator)

# Check if token obtained successfully
if token:
    # Create stats table if not exists
    create_stats_table_if_not_exists()

    # Create historical stats table if not exists
    create_historical_stats_table_if_not_exists()

    while True:
        # Fetch camera stats
        stats_url = "https://esvm.kz/api/v1/stats/cameras"
        stats_data = get_camera_stats(stats_url, token)

        # Check if stats data obtained successfully
        if stats_data is not None:
            latest_total_count, latest_active_count = get_latest_stats_from_db()
            current_total_count = stats_data.get('total_count', 0)
            current_active_count = stats_data.get('active_count', 0)

            if current_total_count != latest_total_count or current_active_count != latest_active_count:
                if update_camera_stats_in_db(stats_data):
                    insert_camera_stats_into_history_db(stats_data)
            else:
                print("No change in stats data.")
        else:
            print("Failed to fetch camera stats.")

        tm.sleep(5)  # Check every second
else:
    print("Token not obtained. Cannot retrieve camera stats.")
