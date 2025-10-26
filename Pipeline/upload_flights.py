#!/usr/bin/env python3

from pymongo import MongoClient
import os 
import requests
from pprint import pprint
import pandas as pd
import time
import db_connection as connection
import datetime
import json
import pandas as pd
from datetime import datetime, timedelta
from requests.exceptions import RequestException, ConnectionError, Timeout, HTTPError

def set_weather(flight):
  API_BASE_URL = "http://api:8000"  # Utiliser l'IP externe si configuré

  #departure
  airportCode_dep = flight['Departure']['AirportCode']
  endpoint = '/airports-by-code?airport_code='+str(airportCode_dep)
  weather_url = ''


  try:
    response = requests.get(f"{API_BASE_URL}{endpoint}", timeout=10)
    time.sleep(0.5)
    if response.status_code == 200:
      airport = response.json()
      if airport:
        #print("Traitement du airportcode "+ str(airportCode_dep))
        if flight.get('Departure', {}).get('Actual'):
          target_time = flight['Departure']['Actual']['Date'] + ' ' + flight['Departure']['Actual']['Time']
        else:
          target_time = flight['Departure']['Scheduled']['Date'] + ' ' + flight['Departure']['Scheduled']['Time']

        target_time = datetime.strptime(target_time, "%Y-%m-%d %H:%M")

        start_date = (target_time - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = (target_time + timedelta(days=1)).strftime("%Y-%m-%d")

        # Requête API
        if flight['future'] == 1:
          weather_url = (
                  "https://api.open-meteo.com/v1/forecast"
                  f"?latitude={airport[0]['Position']['Coordinate']['Latitude']}&longitude={airport[0]['Position']['Coordinate']['Longitude']}"
                  f"&start_date={start_date}&end_date={end_date}"
                  "&hourly=rain,snowfall,temperature_2m,relative_humidity_2m,wind_speed_100m,cloud_cover"
              )
        else:
          weather_url = (
                  "https://archive-api.open-meteo.com/v1/archive"
                  f"?latitude={airport[0]['Position']['Coordinate']['Latitude']}&longitude={airport[0]['Position']['Coordinate']['Longitude']}"
                  f"&start_date={start_date}&end_date={end_date}"
                  "&hourly=rain,snowfall,temperature_2m,relative_humidity_2m,wind_speed_100m,cloud_cover"
              )
        #print(url)
        try:
          response = requests.get(weather_url, timeout=10)
          time.sleep(0.5)
          data = response.json()
          
          # Transformation en DataFrame
          df = pd.DataFrame(data["hourly"])
          df["time"] = pd.to_datetime(df["time"])
          
          # Trouver la ligne dont le timestamp est le plus proche de target_time
          closest = df.iloc[(df["time"] - target_time).abs().argsort()[:1]]
          
          #print("Résultat le plus proche :")
          #closest = closest.drop(['time'])
          weather = closest.to_dict(orient="records")[0]
          flight['Departure']['weather'] = weather
        except Exception as e:
          print("Erreur :", e)
          print("URL :", weather_url)
          flight['Departure']['weather'] = {
            "rain": 0,
            "snowfall": 0,
            "temperature_2m": 0,
            "relative_humidity_2m": 0,
            "wind_speed_100m": 0,
            "cloud_cover": 0,
            "time": target_time
          }
      else:
        url = "https://api.lufthansa.com/v1/mds-references/airports/"+str(airportCode_dep)

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
              airport = airport['AirportResource']['Airports']['Airport']
              #print("Traitement du airportcode "+ str(airportCode_dep))

###

              #print(airport['AirportResource']['Airports']['Airport'])
              if flight.get('Departure', {}).get('Actual'):
                target_time = flight['Departure']['Actual']['Date'] + ' ' + flight['Departure']['Actual']['Time']
              else:
                target_time = flight['Departure']['Scheduled']['Date'] + ' ' + flight['Departure']['Scheduled']['Time']
              target_time = datetime.strptime(target_time, "%Y-%m-%d %H:%M")

              start_date = (target_time - timedelta(days=1)).strftime("%Y-%m-%d")
              end_date = (target_time + timedelta(days=1)).strftime("%Y-%m-%d")

              # Requête API
              if flight['future'] == 1:
                weather_url = (
                        "https://api.open-meteo.com/v1/forecast"
                        f"?latitude={airport['Position']['Coordinate']['Latitude']}&longitude={airport['Position']['Coordinate']['Longitude']}"
                        f"&start_date={start_date}&end_date={end_date}"
                        "&hourly=rain,snowfall,temperature_2m,relative_humidity_2m,wind_speed_100m,cloud_cover"
                    )
              else:
                weather_url = (
                        "https://archive-api.open-meteo.com/v1/archive"
                        f"?latitude={airport['Position']['Coordinate']['Latitude']}&longitude={airport['Position']['Coordinate']['Longitude']}"
                        f"&start_date={start_date}&end_date={end_date}"
                        "&hourly=rain,snowfall,temperature_2m,relative_humidity_2m,wind_speed_100m,cloud_cover"
                    )
              #print(url)
              try:
                response = requests.get(weather_url, timeout=10)
                time.sleep(0.5)
                data = response.json()
                
                # Transformation en DataFrame
                df = pd.DataFrame(data["hourly"])
                df["time"] = pd.to_datetime(df["time"])
                
                # Trouver la ligne dont le timestamp est le plus proche de target_time
                closest = df.iloc[(df["time"] - target_time).abs().argsort()[:1]]
                
                #print("Résultat le plus proche :")
                #closest = closest.drop(['time'])
                weather = closest.to_dict(orient="records")[0]
                flight['Departure']['weather'] = weather
              except Exception as e:
                print("Erreur :", e)
                print("URL :", weather_url)
                flight['Departure']['weather'] = {
                  "rain": 0,
                  "snowfall": 0,
                  "temperature_2m": 0,
                  "relative_humidity_2m": 0,
                  "wind_speed_100m": 0,
                  "cloud_cover": 0,
                  "time": target_time
                }
###

          else:
            #return None
            print(f"Erreur avec le code IATA : " +str(airportCode_dep))
        except RequestException as e:
            print(f"Erreur lors de la requête : {e}")
            data = None  # Gérer l'échec


    else:
      print(airport, response.status_code, response.reason, response.text)
  except requests.exceptions.HTTPError as err:
      print(response.status_code)
      print(f"Erreur lors de la requête : {err}")
      data = None  # Gérer l'échec

  #arrival
  airportCode_arr = flight['Arrival']['AirportCode']
  endpoint = '/airports-by-code?airport_code='+str(airportCode_arr)

  try:
    response = requests.get(f"{API_BASE_URL}{endpoint}", timeout=10)
    time.sleep(0.5)
    if response.status_code == 200:
      airport = response.json()
      if airport:
        #print("Traitement du airportcode "+ str(airportCode_arr))
        #print(airport)
        
        #print(airport[0]['Position']['Coordinate'])

        if flight.get('Arrival', {}).get('Actual'):
          target_time = flight['Arrival']['Actual']['Date'] + ' ' + flight['Arrival']['Actual']['Time']
        else:
          target_time = flight['Arrival']['Scheduled']['Date'] + ' ' + flight['Arrival']['Scheduled']['Time']

        target_time = datetime.strptime(target_time, "%Y-%m-%d %H:%M")

        start_date = (target_time - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = (target_time + timedelta(days=1)).strftime("%Y-%m-%d")

        # Requête API
        if flight['future'] == 1:
          weather_url = (
                  "https://api.open-meteo.com/v1/forecast"
                  f"?latitude={airport[0]['Position']['Coordinate']['Latitude']}&longitude={airport[0]['Position']['Coordinate']['Longitude']}"
                  f"&start_date={start_date}&end_date={end_date}"
                  "&hourly=rain,snowfall,temperature_2m,relative_humidity_2m,wind_speed_100m,cloud_cover"
              )
        else:
          weather_url = (
                  "https://archive-api.open-meteo.com/v1/archive"
                  f"?latitude={airport[0]['Position']['Coordinate']['Latitude']}&longitude={airport[0]['Position']['Coordinate']['Longitude']}"
                  f"&start_date={start_date}&end_date={end_date}"
                  "&hourly=rain,snowfall,temperature_2m,relative_humidity_2m,wind_speed_100m,cloud_cover"
              )
        #print(url)
        try:
          response = requests.get(weather_url, timeout=10)
          time.sleep(0.5)
          data = response.json()
          
          # Transformation en DataFrame
          df = pd.DataFrame(data["hourly"])
          df["time"] = pd.to_datetime(df["time"])
          
          # Trouver la ligne dont le timestamp est le plus proche de target_time
          closest = df.iloc[(df["time"] - target_time).abs().argsort()[:1]]
          
          #print("Résultat le plus proche :")
          #closest = closest.drop(['time'])
          weather = closest.to_dict(orient="records")[0]
          flight['Arrival']['weather'] = weather
        except Exception as e:
          print("Erreur :", e)
          print("URL :", weather_url)
          flight['Arrival']['weather'] = {
            "rain": 0,
            "snowfall": 0,
            "temperature_2m": 0,
            "relative_humidity_2m": 0,
            "wind_speed_100m": 0,
            "cloud_cover": 0,
            "time": target_time
          }
      else:
        url = "https://api.lufthansa.com/v1/mds-references/airports/"+str(airportCode_arr)

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
              airport = airport['AirportResource']['Airports']['Airport']
              #print("Traitement du airportcode "+ str(airportCode_arr))

              if flight.get('Arrival', {}).get('Actual'):
                target_time = flight['Arrival']['Actual']['Date'] + ' ' + flight['Arrival']['Actual']['Time']
              else:
                target_time = flight['Arrival']['Scheduled']['Date'] + ' ' + flight['Arrival']['Scheduled']['Time']

              target_time = datetime.strptime(target_time, "%Y-%m-%d %H:%M")

              start_date = (target_time - timedelta(days=1)).strftime("%Y-%m-%d")
              end_date = (target_time + timedelta(days=1)).strftime("%Y-%m-%d")

              # Requête API
              if flight['future'] == 1:
                weather_url = (
                        "https://api.open-meteo.com/v1/forecast"
                        f"?latitude={airport['Position']['Coordinate']['Latitude']}&longitude={airport['Position']['Coordinate']['Longitude']}"
                        f"&start_date={start_date}&end_date={end_date}"
                        "&hourly=rain,snowfall,temperature_2m,relative_humidity_2m,wind_speed_100m,cloud_cover"
                    )
              else:
                weather_url = (
                        "https://archive-api.open-meteo.com/v1/archive"
                        f"?latitude={airport['Position']['Coordinate']['Latitude']}&longitude={airport['Position']['Coordinate']['Longitude']}"
                        f"&start_date={start_date}&end_date={end_date}"
                        "&hourly=rain,snowfall,temperature_2m,relative_humidity_2m,wind_speed_100m,cloud_cover"
                    )
              #print(url)
              try:
                response = requests.get(weather_url, timeout=10)
                time.sleep(0.5)
                data = response.json()
                
                # Transformation en DataFrame
                df = pd.DataFrame(data["hourly"])
                df["time"] = pd.to_datetime(df["time"])
                
                # Trouver la ligne dont le timestamp est le plus proche de target_time
                closest = df.iloc[(df["time"] - target_time).abs().argsort()[:1]]
                
                #print("Résultat le plus proche :")
                #closest = closest.drop(['time'])
                weather = closest.to_dict(orient="records")[0]
                flight['Arrival']['weather'] = weather
              except Exception as e:
                print("Erreur :", e)
                print("URL :", weather_url)
                flight['Arrival']['weather'] = {
                  "rain": 0,
                  "snowfall": 0,
                  "temperature_2m": 0,
                  "relative_humidity_2m": 0,
                  "wind_speed_100m": 0,
                  "cloud_cover": 0,
                  "time": target_time
                }
###
###

          else:
            #return None
            print("Erreur avec le code IATA : " +str(airportCode_arr))
        except RequestException as e:
            print(f"Erreur lors de la requête : {e}")
            data = None  # Gérer l'échec

  except requests.exceptions.HTTPError as err:
      print(response.status_code)
      print(f"Erreur lors de la requête : {err}")
      data = None  # Gérer l'échec

def uploadCollection(collection, data, db):
  db[collection].insert_many(data)

def main(dateNow):
  #datetimeNow = datetime.datetime.now()
  #dateNow = datetimeNow.strftime("%Y")+"-"+datetimeNow.strftime("%m")+"-"+datetimeNow.strftime("%d")+"T05:00"
  #dateNow = '2025-10-10T05:00'
  data_to_insert = []
  airports = pd.read_csv('airport_code.csv')
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
  uploadCollection('airport', airports_col, mydb)

  additional_airports = [
    {
      'AirportCode': 'FRA',
      'Position': { 'Coordinate': { 'Latitude': 50.03333, 'Longitude': 8.57056 } },
      'CityCode': 'FRA',
      'CountryCode': 'DE',
      'LocationType': 'Airport',
      'Names': {
        'Name': [
          { '@LanguageCode': 'FR', '$': 'Frankfurt' },
        ]
      },
      'UtcOffset': '+01:00',
      'TimeZoneId': 'Europe/Vienna'
    },
    {
      'AirportCode': 'BER',
      'Position': { 'Coordinate': { 'Latitude': 52.36667, 'Longitude': 13.50333 } },
      'CityCode': 'BER',
      'CountryCode': 'DE',
      'LocationType': 'Airport',
      'Names': {
        'Name': [
          { '@LanguageCode': 'FR', '$': 'Berlin' },
        ]
      },
      'UtcOffset': '+01:00',
      'TimeZoneId': 'Europe/Vienna'
    },
    {
      'AirportCode': 'IAS',
      'Position': { 'Coordinate': { 'Latitude': 47.18028, 'Longitude': 27.62083 } },
      'CityCode': 'IAS',
      'CountryCode': 'RO',
      'LocationType': 'Airport',
      'Names': {
        'Name': [
          { '@LanguageCode': 'FR', '$': 'Iasi International Airport' },
        ]
      },
      'UtcOffset': '+01:00',
      'TimeZoneId': 'Europe/Vienna'
    },
    {
      'AirportCode': 'SKP',
      'Position': { 'Coordinate': { 'Latitude': 47.18028, 'Longitude': 27.62083 } },
      'CityCode': 'SKP',
      'CountryCode': 'MAC',
      'LocationType': 'Airport',
      'Names': {
        'Name': [
          { '@LanguageCode': 'FR', '$': 'Skopje International Airport' },
        ]
      },
      'UtcOffset': '+01:00',
      'TimeZoneId': 'Europe/Vienna'
    } 
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
            set_weather(flight)
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
            set_weather(flight)
            data_to_insert.append(flight)

          #if len(data) > 0: 
            #client["sample"]["flight"].insert_many(data)
      else:
        print(datetime.now(), airport, response.status_code, response.reason, response.text)
    except requests.exceptions.HTTPError as err:
      print(response.status_code)
      print(f"{datetime.now()}Erreur lors de la requête : {err}")
      data = None  # Gérer l'échec
  
  os.environ['nb_flight'] = len(data_to_insert)
  print(datetime.now(), os.environ['nb_flight'])
  client["sample"]["flight"].insert_many(data_to_insert)
  print(str(datetime.now()) + ": End upload flight flight mgt")

#main()