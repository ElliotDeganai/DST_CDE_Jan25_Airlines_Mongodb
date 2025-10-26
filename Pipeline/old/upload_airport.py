#!/usr/bin/env python3

from pymongo import MongoClient
import os 
import requests
from pprint import pprint
import time
import json
import db_connection as connection

client = connection.client_vps
#client = connection.client_dst

########################################################## Flights data
def getAirportCodeList(codes):
  res = []
  for code in codes:
    res.append(code['code'])  
  return list(set(res))

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
  db[collection].insert_many(data)

def main():
  mydb = client["sample"]
  airports = mydb['airport']
  flights = mydb['flight']

  #flights_json = loadFlights(mydb)

  codea = flights.aggregate([    
    {
      "$group":
        {
          "_id": "$Arrival.AirportCode"
        }
    },
    {"$project" : {"_id":0,"code": "$_id"}}
    ]
  )

  coded = flights.aggregate([    
    {
      "$group":
        {
          "_id": "$Departure.AirportCode"
        }
    },
    {"$project" : {"_id":0,"code": "$_id"}}
    ]
  )

  airport_codes = []
  airport_codes = list(codea)+list(coded)

  airport_codes = getAirportCodeList(airport_codes)

  airports_col = []

  weather_col = []

  ##########Add the weather collection with the airport data

  for airport_code in airport_codes:

    airport = getAirportApi(airport_code)
    if airport != None:
      airports_col.append(getAirportApi(airport_code))

    airport_position = getAirportCoordApi(airport_code)
    if airport_position:

      url = "https://api.openweathermap.org/data/2.5/forecast?lat="+str(airport_position['Latitude'])+"&lon="+str(airport_position['Longitude'])+"&appid=ddfb4b4c01898ee0681311b1649181b9"

      payload = {}
      headers = {}

      try:
        response = requests.request("GET", url, headers=headers, data=payload)
        weather = response.json()
        weather_forecast_list = weather['list']

        for weather_forecast in weather_forecast_list:
          weather_forecast['AirportCode'] = airport_code
          weather_col.append(weather_forecast)
      except RequestException as e:
        print(f"Erreur lors de la requête : {e}")
        data = None

  uploadCollection('weather', weather_col, mydb)
  uploadCollection('airport', airports_col, mydb)

#main()