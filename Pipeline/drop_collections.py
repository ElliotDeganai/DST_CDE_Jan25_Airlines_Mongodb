#!/usr/bin/env python3

from pymongo import MongoClient
import os
import requests
from pprint import pprint
import db_connection as connection
from datetime import datetime, timedelta

#client = connection.client_vps
client = connection.client_dst
#collections = ['airport', 'city', 'country', 'flight', 'weather']
#collections = ['weather']

def main(collections):

    mydb = client["sample"]
    print(str(datetime.now()) + ": Debut du drop collection.")
    for col in collections:
        col_to_delete = mydb[col]
        col_to_delete.drop()
        print(str(datetime.now()) + ": Collection " + col +" a été supprimé.")

    print(str(datetime.now()) + ": Fin du drop collection.")


if __name__ == "__main__":
    main()
