import psycopg2
import requests
from requests.auth import HTTPBasicAuth
import os
from datetime import datetime, timedelta
import logging
from tqdm import tqdm

# Database connection details
db_host = "10.100.200.102"
db_port = "5439"
db_name = "sitcenter_postgis_datalake"
db_user = "la_noche_estrellada"
db_password = "Cfq,thNb13@"

# Authentication details
login = "cra_api@esvm.kz"
password = "qyKoZ7wosJf2W7AhOFINz5clCyOdKtD0"

# Create a unique directory to save images based on the current timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
directory_name = f"downloaded_images_{timestamp}"
os.makedirs(directory_name, exist_ok=True)

# Set up logging
logging.basicConfig(
    filename=f"{directory_name}/download.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    dbname=db_name,
    user=db_user,
    password=db_password
)
cursor = conn.cursor()

# Calculate the time 5 minutes ago
time_5_minutes_ago = datetime.now() - timedelta(minutes=5)

# Query to fetch the most recent snapshot URLs from the last 5 minutes
query = """
    SELECT snapshot_url
    FROM esvm_events_lpr
    WHERE snapshot_url IS NOT NULL
    AND event_ts >= %s
    AND event_ts::date = CURRENT_DATE
    ORDER BY event_ts DESC
"""
cursor.execute(query, (time_5_minutes_ago,))

# Fetch all rows
rows = cursor.fetchall()

# Download each image with progress bar
for row in tqdm(rows, desc="Downloading images", unit="image"):
    url = row[0]
    image_name = url.split("/")[-1].split("?")[0]
    try:
        response = requests.get(url, auth=HTTPBasicAuth(login, password))
        response.raise_for_status()

        with open(f"{directory_name}/{image_name}", "wb") as file:
            file.write(response.content)
        logging.info(f"Successfully downloaded {image_name} from {url}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download {image_name} from {url}: {e}")

# Close the database connection
cursor.close()
conn.close()
