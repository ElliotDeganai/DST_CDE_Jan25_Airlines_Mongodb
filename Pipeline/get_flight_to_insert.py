#!/usr/bin/env python3

from pymongo import MongoClient
import os 
import requests
from pprint import pprint
import pandas as pd
import time
import db_connection as connection
import set_weather
import datetime
import json
import pandas as pd
from datetime import datetime, timedelta
from requests.exceptions import RequestException, ConnectionError, Timeout, HTTPError

def uploadCollection(collection, data, db):
  if len(data):
    db[collection].insert_many(data)
  else:
    print(str(datetime.now()) + ": l'array" + str(data) + " est vide il n'y a pas de données à insérer.")

def main(dateNow, data_to_insert):
  #datetimeNow = datetime.datetime.now()
  #dateNow = datetimeNow.strftime("%Y")+"-"+datetimeNow.strftime("%m")+"-"+datetimeNow.strftime("%d")+"T05:00"
  #dateNow = '2025-10-10T05:00'
  #data_to_insert = []
  airports = pd.read_csv('airport_code2.csv')
  print(str(datetime.now()) + ": Start upload flight airport code management")

  #client = connection.client_vps
  
  airports_col = []
  client = connection.client_dst

  for airport in airports['IATA Code']:
    url = "https://api.lufthansa.com/v1/mds-references/airports/"+str(airport)

    payload = {}
    headers = {
      'Authorization': 'Bearer '+str(os.environ['lufthansa_token'])
    }

    try:
      response = requests.request("GET", url, headers=headers, data=payload)
      time.sleep(0.5)

      if response.status_code == 200:
        airport = response.json()
        if airport:
          airports_col.append(airport['AirportResource']['Airports']['Airport'])
      else:
        #return None
        print(str(datetime.now()) + ": Erreur avec le code IATA : " +str(airport))
    except RequestException as e:
        print(f"{datetime.now()} Erreur lors de la requête : {e}")
        data = None  # Gérer l'échec


  mydb = client["sample"]
  set_weather.uploadCollection('airport', airports_col, mydb)

  additional_airports = [
  ]
  
  uploadCollection('airport', additional_airports, mydb)
  
  print(str(datetime.now()) + ": End upload flight airport code management")

  
  print(str(datetime.now()) + ": Start upload flight flight mgt")
  for airport in airports['IATA Code']:
    url = "https://api.lufthansa.com/v1/operations/customerflightinformation/departures/"+str(airport)+"/"+str(dateNow)
    payload = {}
    headers = {
      'Authorization': 'Bearer '+str(os.environ['lufthansa_token'])
    }
    try:
      response = requests.request("GET", url, headers=headers, data=payload)
      time.sleep(0.5)
      if response.status_code == 200:
        flights = response.json()
        if flights:
          data = flights['FlightInformation']['Flights']['Flight']

          # 🩵 S’assurer que "data" soit toujours une liste
          if isinstance(data, dict):
              data = [data]
          for flight in data:
            if flight.get('Departure', {}).get('Actual'):
              target_time = flight['Departure']['Actual']['Date'] + ' ' + flight['Departure']['Actual']['Time']
            else:
              target_time = flight['Departure']['Scheduled']['Date'] + ' ' + flight['Departure']['Scheduled']['Time']
            target_time = datetime.strptime(target_time, "%Y-%m-%d %H:%M")
            if target_time > datetime.now():
              flight['future'] = 1
            else :
              flight['future'] = 0
            #print(flight)
            set_weather.main(flight)
            data_to_insert.append(flight)

          #if len(data) > 0: 
            #client["sample"]["flight"].insert_many(data)
      else:
        print(str(datetime.now()), airport, response.status_code, response.reason, response.text)
    except requests.exceptions.HTTPError as err:
      print(response.status_code) 
      print(f"{datetime.now()}: Erreur lors de la requête : {err}")
      data = None  # Gérer l'échec

    url = "https://api.lufthansa.com/v1/operations/customerflightinformation/arrivals/"+str(airport)+"/"+str(dateNow)
    payload = {}
    headers = {
      'Authorization': 'Bearer '+str(os.environ['lufthansa_token'])
    }
    try:
      response = requests.request("GET", url, headers=headers, data=payload)
      time.sleep(0.5)
      if response.status_code == 200:
        flights = response.json()
        if flights:
          data = flights['FlightInformation']['Flights']['Flight']

          # 🩵 S’assurer que "data" soit toujours une liste
          if isinstance(data, dict):
              data = [data]

          for flight in data:
            if flight.get('Departure', {}).get('Actual'):
              target_time = flight['Departure']['Actual']['Date'] + ' ' + flight['Departure']['Actual']['Time']
            else:
              target_time = flight['Departure']['Scheduled']['Date'] + ' ' + flight['Departure']['Scheduled']['Time']
            target_time = datetime.strptime(target_time, "%Y-%m-%d %H:%M")
            if target_time > datetime.now():
              flight['future'] = 1
            else :
              flight['future'] = 0
            #print(flight)
            set_weather.main(flight)
            data_to_insert.append(flight)

          #if len(data) > 0: 
            #client["sample"]["flight"].insert_many(data)
      else:
        print(datetime.now(), airport, response.status_code, response.reason, response.text)
    except requests.exceptions.HTTPError as err:
      print(response.status_code)
      print(f"{datetime.now()}Erreur lors de la requête : {err}")
      data = None  # Gérer l'échec
  
  
  print(datetime.now(), os.environ['nb_flight'])
  #client["sample"]["flight"].insert_many(data_to_insert)
  print(str(datetime.now()) + ": End flight retrieval flight flight mgt")

if __name__ == "__main__":
    # Ici, tu choisis ce qui se passe si tu exécutes le script directement :
    import sys
    # Exemple : arguments de la ligne de commande
    args = sys.argv[1:]
    main(*args)