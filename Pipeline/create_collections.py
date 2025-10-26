#!/usr/bin/env python3

from pymongo import MongoClient
import os 
import requests
from pprint import pprint
import db_connection as connection
from datetime import datetime, timedelta

#client = connection.client_vps
client = connection.client_dst

def main(collections):

    mydb = client["sample"]

    print(str(datetime.now()) + ": Debut du create collection.")
    for col in collections:
        rand = mydb.create_collection(name=col)
        print(str(datetime.now()) + ": Collection " + col +" a été créé.")

    print(str(datetime.now()) + ": Fin du create collection.")

if __name__ == "__main__":
    main()