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


#client = connection.client_vps
client = connection.client_dst



########################################################## Flights data


def loadFlights(db):
  url = "https://api.lufthansa.com/v1/operations/customerflightinformation/departures/CDG/2025-07-26T05:00"
  payload = {}
  headers = {
    'Authorization': 'Bearer '+str(os.environ['lufthansa_token'])
  }
  airport_codes = []

  try:
    response = requests.request("GET", url, headers=headers, data=payload)
    time.sleep(0.5)

    if response.status_code == 200:
    
      flights = response.json()
      data = flights['FlightInformation']['Flights']['Flight']
      #rand = db.create_collection(name="flight")
      mydb['flight'].insert_many(data)
      #input_dict = json.loads(data)

      #return input_dict
      
    else:
      print(response.status_code, response.reason, response.text)
      return None
  except requests.exceptions.HTTPError as err:
    print(response.status_code)
    print(f"Erreur lors de la requête : {err}")
    data = None  # Gérer l'échec

def getAirportCodeList(codes):
  res = []
  for code in codes:
    res.append(code['code'])  
  return list(set(res))

def getAirlineApi(AirlineCode):
  url = "https://api.lufthansa.com/v1/mds-references/airlines/"+str(AirlineCode)

  payload = {}
  headers = {
    'Authorization': 'Bearer '+str(os.environ['lufthansa_token'])
  }
  try:
    response = requests.request("GET", url, headers=headers, data=payload)
    time.sleep(0.5)

    if response.status_code == 200:
      country = response.json()
      if country:
        return country['AirlineResource']['Airlines']['Airline']
    else:
      return None
  except RequestException as e:
      print(f"Erreur lors de la requête : {e}")
      data = None  # Gérer l'échec

def getCountryApi(CountryCode):
  url = "https://api.lufthansa.com/v1/mds-references/countries/"+str(CountryCode)

  payload = {}
  headers = {
    'Authorization': 'Bearer '+str(os.environ['lufthansa_token'])
  }
  try:
    response = requests.request("GET", url, headers=headers, data=payload)
    time.sleep(0.5)

    if response.status_code == 200:
      country = response.json()
      if country:
        return country['CountryResource']['Countries']['Country']
    else:
      return None
  except RequestException as e:
      print(f"Erreur lors de la requête : {e}")
      data = None  # Gérer l'échec

def getCityApi(CityCode):
  url = "https://api.lufthansa.com/v1/mds-references/cities/"+str(CityCode)

  payload = {}
  headers = {
    'Authorization': 'Bearer '+str(os.environ['lufthansa_token'])
  }

  try:
    response = requests.request("GET", url, headers=headers, data=payload)
    time.sleep(0.5)

    if response.status_code == 200:
      city = response.json()
      if city:
        return city['CityResource']['Cities']['City']
    else:
      return None
  except RequestException as e:
      print(f"Erreur lors de la requête : {e}")
      data = None  # Gérer l'échec

def getAirportApi(AirportCode):
  url = "https://api.lufthansa.com/v1/mds-references/airports/"+str(AirportCode)

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
        return airport['AirportResource']['Airports']['Airport']
    else:
      return None
  except RequestException as e:
      print(f"Erreur lors de la requête : {e}")
      data = None  # Gérer l'échec

def getCountryCodeFromAirportApi(AirportCode):
  url = "https://api.lufthansa.com/v1/mds-references/airports/"+str(AirportCode)

  payload = {}
  headers = {
    'Authorization': 'Bearer '+str(os.environ['lufthansa_token'])
  }

  try:
    response = requests.get(url, headers=headers, data=payload)
    time.sleep(0.5)

    if response.status_code == 200:
      airport = response.json()
      return airport['AirportResource']['Airports']['Airport']['CountryCode']
    else:
      print(response.status_code, response.reason, response.text)
      return None
  except requests.exceptions.HTTPError as err:
    print(response.status_code)
    print(f"Erreur lors de la requête : {err}")
    data = None  # Gérer l'échec

def getCityCodeFromAirportApi(AirportCode):
  url = "https://api.lufthansa.com/v1/mds-references/airports/"+str(AirportCode)

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
        return airport['AirportResource']['Airports']['Airport']['CityCode']
    else:
      return None
  except RequestException as e:
    print(f"Erreur lors de la requête : {e}")
    data = None  # Gérer l'échec

def getAirportCoordApi(AirportCode):
  url = "https://api.lufthansa.com/v1/mds-references/airports/"+str(AirportCode)

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
        return airport['AirportResource']['Airports']['Airport']['Position']['Coordinate']
    else:
      return None
  except RequestException as e:
    print(f"Erreur lors de la requête : {e}")
    data = None  # Gérer l'échec

def uploadCollection(collection, data, db):
  if len(data):
    db[collection].insert_many(data)
  else:
    print(str(datetime.now()) + ": l'array" + str(data) + " est vide il n'y a pas de données à insérer.")

def get_missing_airports():
  
  client = connection.client_dst
  db = client["sample"]
  flight_col = db["flight"]    # ta collection de vols
  airport_col = db["airport"]    # ta collection des aéroports

  # 1️⃣ Récupère les codes distincts de départ et d'arrivée
  departure_codes = flight_col.distinct("Departure.AirportCode")
  arrival_codes = flight_col.distinct("Arrival.AirportCode")

  # 2️⃣ Combine et supprime les doublons
  all_flight_codes = set(departure_codes + arrival_codes)

  # 3️⃣ Récupère les codes déjà présents dans la collection 'airport'
  existing_codes = set(airport_col.distinct("AirportCode"))

  # 4️⃣ Retire ceux déjà existants
  new_codes = list(all_flight_codes - existing_codes)
  #print(new_codes)
  return new_codes

def get_missing_airlines():
  
  client = connection.client_dst
  db = client["sample"]
  flight_col = db["flight"]    # ta collection de vols
  airline_col = db["airline"]    # ta collection des aéroports

  # 1️⃣ Récupère les codes distincts de départ et d'arrivée
  airlines = flight_col.distinct("OperatingCarrier.AirlineID")
  airlines = set(airlines)

  # 3️⃣ Récupère les codes déjà présents dans la collection 'airport'
  existing_codes = set(airline_col.distinct("OperatingCarrier.AirlineID"))

  # 4️⃣ Retire ceux déjà existants
  new_codes = list(airlines - existing_codes)
  #print(new_codes)
  return new_codes

def main():
  print(str(datetime.now()) + ": start upload other data upload")
  mydb = client["sample"]
  airports = mydb['airport']
  flights = mydb['flight']

  airport_codes = get_missing_airports()
  
  print(str(datetime.now()) + ": Missing airports: " + str(airport_codes))
  print(airport_codes)

  airports_col = []

  country_codes = []
  country_col = []

  city_codes = []
  city_col = []

  airline_col = []

  airline_codes = get_missing_airlines()

  ##########Add the weather collection with the airport data

  for airport_code in airport_codes:

    print(airport_code)

    airport = getAirportApi(airport_code)
    #print(airport, airport_code)
    if airport != None:
      airports_col.append(getAirportApi(airport_code))

    country_code = getCountryCodeFromAirportApi(airport_code)
    #print(airport, country_code)
    if country_code:    
      country_codes.append(country_code)

    city_code = getCityCodeFromAirportApi(airport_code)
    if city_code:
      city_codes.append(city_code)

  country_codes = list(set(country_codes))
  city_codes = list(set(city_codes))

  for code_co in country_codes:
    country = getCountryApi(code_co)
    if country != None:
      country_col.append(getCountryApi(code_co))

  for code_ci in city_codes:
    city = getCityApi(code_ci)
    if city != None:
      city_col.append(city)

  for airline_code in airline_codes:
    airline = getAirlineApi(airline_code)
    if airline != None:
      airline_col.append(airline)


  print(country_col)
  if len(country_col) > 0:
    uploadCollection('country', country_col, mydb)  
  print(str(datetime.now()) + ": Country loaded")
  if len(city_col) > 0:
    uploadCollection('city', city_col, mydb)  
  print(str(datetime.now()) + ": City loaded")
  #uploadCollection('weather', weather_col, mydb) 
  if len(airports_col) > 0: 
    uploadCollection('airport', airports_col, mydb)
  if len(airline_col) > 0: 
    uploadCollection('airline', airline_col, mydb)
  print(str(datetime.now()) + ": Airport loaded")



if __name__ == "__main__":
    main()