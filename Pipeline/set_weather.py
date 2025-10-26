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

def main(flight):
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
  if len(data) > 0:
    db[collection].insert_many(data)
  else:
    print(str(datetime.now()) + ": l'array" + str(data) + " est vide il n'y a pas de données à insérer.")

if __name__ == "__main__":
    # Ici, tu choisis ce qui se passe si tu exécutes le script directement :
    import sys
    # Exemple : arguments de la ligne de commande
    args = sys.argv[1:]
    main(*args)