#!/usr/bin/env python3

from pymongo import MongoClient
import os
import requests
from pprint import pprint

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
