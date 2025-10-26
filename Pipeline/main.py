#!/usr/bin/env python3
import drop_collections
import create_collections
import upload_flights
import update_future
import load_stats
import get_flight_to_insert
import upload_other_data
import get_flight_to_insert
import get_access_token
import logging
import os
from datetime import datetime, timedelta
from pymongo import MongoClient

MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_USER = os.getenv("MONGO_USER", "datascientest")
MONGO_PASS = os.getenv("MONGO_PASS", "dst123")

# Connection to the DB
client_dst = MongoClient(
    host = MONGO_HOST,
    port = MONGO_PORT,
    username = MONGO_USER,
    password = MONGO_PASS
)

client_vps = MongoClient(
    host = MONGO_HOST,
    port = MONGO_PORT,
    username = MONGO_USER,
    password = MONGO_PASS
)

client = client_dst

mydb = client["sample"]
collection = mydb["flight"]

# logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, "pipeline_mongo.log")),
        logging.StreamHandler()  # Still prints to stdout for Docker logs
    ]
)

logger = logging.getLogger(__name__)

collectionsDelete = ['airport', 'city', 'country', 'weather', 'flight', 'airline', 'statistics','statistics_by_airline', 'statistics_by_airport_dep', 'statistics_by_airport_arr', 'param']

drop_collections.main(collectionsDelete)

collectionsCreate = ['airport', 'city', 'country', 'weather', 'flight', 'airline', 'statistics','statistics_by_airline', 'statistics_by_airport_dep', 'statistics_by_airport_arr', 'param']
os.environ['nb_flight'] = str(1000)
client["sample"]["param"].insert_one({"flight_count": 0})

create_collections.main(collectionsCreate)

#collectionsDelete = ['weather']

#drop_collections.main(collectionsDelete)

#collectionsCreate = ['weather']

#create_collections.main(collectionsCreate)

get_access_token.main()

#upload_airport.main()

#date_list = ['2025-10-10T05:00', '2025-10-10T12:00', '2025-10-10T10:00', '2025-10-10T15:00', '2025-10-10T20:00', '2025-10-09T05:00', '2025-10-09T12:00', '2025-10-09T15:00', '2025-10-09T20:00']

update_future.main()

date_list = ['2025-10-26T15:00', '2025-10-27T05:00']
flight_to_insert = []
print(str(datetime.now()) + ": Debut du create collection.")

for dateNow in date_list:
    get_flight_to_insert.main(dateNow, flight_to_insert)

os.environ['nb_flight'] = str(len(flight_to_insert))
client["sample"]["flight"].insert_many(flight_to_insert)

if collection.count_documents({}) >= int(os.environ['nb_flight']):
    #client["sample"]["param"].insert_one({"flight_count": int(os.environ['nb_flight'])})
    client["sample"]["param"].update_one(
        {},   # Identifie le document
        {"$set": {
            'flight_count': int(os.environ['nb_flight'])
            }
        }  # Ajoute ou met à jour
    )
    #upload_flights.main()
    upload_other_data.main()
    load_stats.main()
