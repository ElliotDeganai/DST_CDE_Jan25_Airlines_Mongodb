#!/usr/bin/env python3

from pymongo import MongoClient
import os 
import requests
from pprint import pprint
import db_connection as connection
import pandas as pd
import numpy as np
from flatten_json import flatten, unflatten
from bson import ObjectId
from datetime import datetime, timedelta

#client = connection.client_vps
API_BASE_URL = os.getenv("API_BASE_URL", "http://api:8000")  # Utiliser l'IP externe si configuré
endpoint = '/flights-all'

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

def main():

    mydb = client["sample"]
    col_to_delete = mydb['statistics']
    col_to_delete.drop()
    col_to_delete = mydb['statistics_by_airline']
    col_to_delete.drop()
    col_to_delete = mydb['statistics_by_airport_dep']
    col_to_delete.drop()
    col_to_delete = mydb['statistics_by_airport_arr']
    col_to_delete.drop()
    rand = mydb.create_collection(name='statistics')
    rand = mydb.create_collection(name='statistics_by_airline')
    rand = mydb.create_collection(name='statistics_by_airport_dep')
    rand = mydb.create_collection(name='statistics_by_airport_arr')
    flight_col = mydb["flight"]
    stats_col = mydb["statistics"]
    stats_airline_col = mydb["statistics_by_airline"]
    stats_airport_dep_col = mydb["statistics_by_airport_dep"]
    stats_airport_arr_col = mydb["statistics_by_airport_arr"]
    stats_col.insert_one({
            'mean': 0,
            'min': 0,
            'max': 0,
            'pct_retarded': 0,
            'pct_retarded_sup_15': 0,
            'pct_retarded_sup_30': 0,
            'pct_retarded_sup_60': 0,
            'cnt_retarded': 0,
            'cnt_retarded_sup_15': 0,
            'cnt_retarded_sup_30': 0,
            'cnt_retarded_sup_60': 0,
            })

    print('Start data retrieval from API to load stats')
    try:
        response = requests.get(f"{API_BASE_URL}{endpoint}", timeout=60)
        if response.status_code == 200:
            dict_flattened = (flatten(record, '_') for record in response.json())
            df = pd.DataFrame(dict_flattened)
            df_minimized = df.drop(['MarketingCarrierList_MarketingCarrier_AirlineID',
                'MarketingCarrierList_MarketingCarrier_FlightNumber',
                'MarketingCarrierList_MarketingCarrier_0_AirlineID',
                'MarketingCarrierList_MarketingCarrier_0_FlightNumber',
                'MarketingCarrierList_MarketingCarrier_1_AirlineID',
                'MarketingCarrierList_MarketingCarrier_1_FlightNumber',
                'MarketingCarrierList_MarketingCarrier_2_AirlineID',
                'MarketingCarrierList_MarketingCarrier_2_FlightNumber',
                'MarketingCarrierList_MarketingCarrier_3_AirlineID',
                'MarketingCarrierList_MarketingCarrier_3_FlightNumber',
                'MarketingCarrierList_MarketingCarrier_4_AirlineID',
                'MarketingCarrierList_MarketingCarrier_4_FlightNumber',
                'MarketingCarrierList_MarketingCarrier_5_AirlineID',
                'MarketingCarrierList_MarketingCarrier_5_FlightNumber',
                'MarketingCarrierList_MarketingCarrier_6_AirlineID',
                'MarketingCarrierList_MarketingCarrier_6_FlightNumber',
                'MarketingCarrierList_MarketingCarrier_7_AirlineID',
                'MarketingCarrierList_MarketingCarrier_7_FlightNumber',
                'MarketingCarrierList_MarketingCarrier_8_AirlineID',
                'MarketingCarrierList_MarketingCarrier_8_FlightNumber',
                'MarketingCarrierList_MarketingCarrier_9_AirlineID',
                'MarketingCarrierList_MarketingCarrier_9_FlightNumber',
                'MarketingCarrierList_MarketingCarrier_10_AirlineID',
                'MarketingCarrierList_MarketingCarrier_10_FlightNumber',
                'MarketingCarrierList_MarketingCarrier_11_AirlineID',
                'MarketingCarrierList_MarketingCarrier_11_FlightNumber', 'Departure_Terminal_Gate',
                                'Arrival_Terminal_Name', 'Arrival_Terminal_Gate',
                                'Departure_Terminal_Name',
                                'Status_Description', 'Departure_Status_Description', 'Arrival_Status_Description'], axis=1, errors='ignore')
            print(df_minimized.columns)
    except Exception as e:
        print(f"Erreur API {endpoint} : {e}")
    print(df.head())
    print('End data retrieval from API to load stats')
    print('Start delay calculation')
        
    if 'Departure_Actual_Date' in df.columns and 'Departure_Actual_Time' in df.columns:
        df_minimized['Departure_Actual_Datetime'] = pd.to_datetime(df['Departure_Actual_Date'] + ' ' + df['Departure_Actual_Time'])
    else:
        df_minimized['Departure_Actual_Datetime'] = pd.to_datetime(df['Departure_Scheduled_Date'] + ' ' + df['Departure_Scheduled_Time'])
        
    if 'Arrival_Actual_Date' in df.columns and 'Arrival_Actual_Time' in df.columns:
        df_minimized['Arrival_Actual_Datetime'] = pd.to_datetime(df['Arrival_Actual_Date'] + ' ' + df['Arrival_Actual_Time'])
    else:
        df_minimized['Arrival_Actual_Datetime'] = pd.to_datetime(df['Arrival_Scheduled_Date'] + ' ' + df['Arrival_Scheduled_Time'])
    df_minimized['Departure_Scheduled_Datetime'] = pd.to_datetime(df['Departure_Scheduled_Date'] + ' ' + df['Departure_Scheduled_Time'])
    df_minimized['Arrival_Scheduled_Datetime'] = pd.to_datetime(df['Arrival_Scheduled_Date'] + ' ' + df['Arrival_Scheduled_Time'])
    #print(df_minimized.head())

    df_w_datetime = df_minimized.drop(['Departure_Actual_Date', 'Departure_Actual_Time', 'Arrival_Actual_Date', 'Arrival_Actual_Time', 'Departure_Scheduled_Date', 'Departure_Scheduled_Time', 'Arrival_Scheduled_Date', 'Arrival_Scheduled_Time'], axis=1, errors='ignore')

    df_w_datetime['is_delayed'] = df_w_datetime['Arrival_Actual_Datetime'] > df_w_datetime['Arrival_Scheduled_Datetime']

    #df_min['delays'] = df_min['Arrival_Actual_Datetime'] - df_min['Arrival_Scheduled_Datetime']

    df_w_datetime["delay (min)"] = np.where(
        df_w_datetime["is_delayed"],
        (df_w_datetime["Arrival_Actual_Datetime"] - df_w_datetime["Arrival_Scheduled_Datetime"]).astype(int)/(60*(10**9)),
        -(df_w_datetime["Arrival_Scheduled_Datetime"] - df_w_datetime["Arrival_Actual_Datetime"]).astype(int)/(60*(10**9))
    )

    df_w_datetime_red = df_w_datetime.drop(['Arrival_Estimated_Date', 'Arrival_Estimated_Time', 'Departure_Estimated_Date', 'Departure_Estimated_Time'], axis=1, errors='ignore')
    print(df_w_datetime_red['delay (min)'][df_w_datetime_red['delay (min)'] > 0].describe())
    pd_mean = df_w_datetime_red['delay (min)'][df_w_datetime_red['delay (min)'] > 0].mean()
    pd_min = df_w_datetime_red['delay (min)'][df_w_datetime_red['delay (min)'] > 0].min()
    pd_max = df_w_datetime_red['delay (min)'][df_w_datetime_red['delay (min)'] > 0].max()
    pct_retarded = len(df_w_datetime_red['delay (min)'][df_w_datetime_red['delay (min)'] > 0])/len(df_w_datetime_red)
    pct_retarded_sup_15 = len(df_w_datetime_red['delay (min)'][df_w_datetime_red['delay (min)'] >= 15])/len(df_w_datetime_red['delay (min)'][df_w_datetime_red['delay (min)'] > 0])
    pct_retarded_sup_30 = len(df_w_datetime_red['delay (min)'][df_w_datetime_red['delay (min)'] >= 30])/len(df_w_datetime_red['delay (min)'][df_w_datetime_red['delay (min)'] > 0])
    pct_retarded_sup_60 = len(df_w_datetime_red['delay (min)'][df_w_datetime_red['delay (min)'] >= 60])/len(df_w_datetime_red['delay (min)'][df_w_datetime_red['delay (min)'] > 0])
    cnt_retarded = len(df_w_datetime_red['delay (min)'][df_w_datetime_red['delay (min)'] > 0])
    cnt_retarded_sup_15 = len(df_w_datetime_red['delay (min)'][df_w_datetime_red['delay (min)'] >= 15])
    cnt_retarded_sup_30 = len(df_w_datetime_red['delay (min)'][df_w_datetime_red['delay (min)'] >= 30])
    cnt_retarded_sup_60 = len(df_w_datetime_red['delay (min)'][df_w_datetime_red['delay (min)'] >= 60])

    print(str(datetime.now()) + ": start update delay min in db")

    for _, row in df_w_datetime_red.iterrows():
        flight_col.update_one(
            {"_id": ObjectId(row["_id"])},   # Identifie le document
            {"$set": {
                'is_delayed': row['is_delayed'],
                'actual_delay_min': row['delay (min)'],
                }
            }  # Ajoute ou met à jour
        )
    print(str(datetime.now()) + ": end update delay min in db") 

    print(str(datetime.now()) + ": start update stats in db")

    
    stats_col.update_one({},
        { "$set": {
            'mean': pd_mean,
            'min': pd_min,
            'max': pd_max,
            'pct_retarded': pct_retarded,
            'pct_retarded_sup_15': pct_retarded_sup_15,
            'pct_retarded_sup_30': pct_retarded_sup_30,
            'pct_retarded_sup_60': pct_retarded_sup_60,
            'cnt_retarded': cnt_retarded,
            'cnt_retarded_sup_15': cnt_retarded_sup_15,
            'cnt_retarded_sup_30': cnt_retarded_sup_30,
            'cnt_retarded_sup_60': cnt_retarded_sup_60,
            }
        }  # Ajoute ou met à jour
    )
    agg_param = {
        'delay (min)': 'mean'
    }

    df_by_airline = df_w_datetime_red.groupby('OperatingCarrier_AirlineID', as_index=False).agg(agg_param).sort_values('delay (min)', ascending=False)
    df_by_dep_airport = df_w_datetime_red.groupby('Departure_AirportCode', as_index=False).agg(agg_param).sort_values('delay (min)', ascending=False)
    df_by_arr_airport = df_w_datetime_red.groupby('Arrival_AirportCode', as_index=False).agg(agg_param).sort_values('delay (min)', ascending=False)

    json_by_airline = df_by_airline.to_dict(orient="records")
    json_by_dep_airport = df_by_dep_airport.to_dict(orient="records")
    json_by_arr_airport = df_by_arr_airport.to_dict(orient="records")

    if len(json_by_airline) > 0:
        stats_airline_col.insert_many(json_by_airline)

    if len(json_by_dep_airport) > 0:
        stats_airport_dep_col.insert_many(json_by_dep_airport)

    if len(json_by_arr_airport) > 0:
        stats_airport_arr_col.insert_many(json_by_arr_airport)
    
    print(str(datetime.now()) + ": end update stats in db") 

if __name__ == "__main__":
    main()