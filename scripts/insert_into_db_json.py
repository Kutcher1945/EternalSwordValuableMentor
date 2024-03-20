import psycopg2
import json

# Connect to your PostgreSQL database
conn = psycopg2.connect(
    dbname="sitcenter_postgis_datalake",
    user="la_noche_estrellada",
    password="Cfq,thNb13@",
    host="172.30.227.205",
    port="5439"
)

# Open a cursor to perform database operations
cur = conn.cursor()

# Read data from JSON file
with open('id_mapper.json') as json_file:
    data = json.load(json_file)

# Insert data into esvm_events_lpr_make table
for make_data in data["make"]:
    cur.execute("INSERT INTO esvm_events_lpr_make (id, name) VALUES (%s, %s)", (make_data["id"], make_data["name"]))

    # Insert data into esvm_events_lpr_model table associated with the current make
    for model_data in make_data["model"]:
        cur.execute("INSERT INTO esvm_events_lpr_model (id, name, make_id) VALUES (%s, %s, %s)", (model_data["id"], model_data["name"], make_data["id"]))

# Commit the transaction
conn.commit()

# Close communication with the database
cur.close()
conn.close()
