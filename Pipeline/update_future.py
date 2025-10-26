#!/usr/bin/env python3

from pymongo import MongoClient
import os 
import requests
from pprint import pprint
import time
import json
import db_connection as connection
import pandas as pd
from datetime import datetime, timedelta

def main():
    #client = connection.client_vps
    client = connection.client_dst
    mydb = client["sample"]
    collection = mydb["flight"]

    today = datetime.now()

    filter_query = {
        "$or": [
            {"future": 1},
            {"Departure.Scheduled.Date": {"$lt": today}}
        ]
    }

    collection.update_many(
        filter_query,   # Identifie le document
        {"$set": {
            'future': 0
            }
        }  # Ajoute ou met à jour
    )

main()