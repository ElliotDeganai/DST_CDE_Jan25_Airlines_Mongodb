#!/usr/bin/env python3
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import mean_absolute_error, accuracy_score, classification_report, mean_squared_error, r2_score
import requests
import os
from flatten_json import flatten, unflatten
from sklearn.preprocessing import OneHotEncoder
from pymongo import MongoClient
from bson import ObjectId
import time
from datetime import datetime, timedelta

def main():

    API_BASE_URL = os.getenv("API_BASE_URL", "http://api:8000")  # Utiliser l'IP externe si configuré
    endpoint = '/flights-all-past'

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

    mydb = client["sample"]
    collection = mydb["flight"]
    param = mydb["param"]
    #MIN_DOCS = param.find_one()["flight_count"]  # seuil de données à atteindre

        # Attente tant que la DB n'a pas assez de données
    #while collection.count_documents({}) < MIN_DOCS:
        #print(f"Documents dans la DB : {collection.count_documents({})}, en attente de {MIN_DOCS}...")
        #time.sleep(10)  # attend 10 secondes avant de réessayer

    print("Attente du document param.flight_count ...")
    while not param.find_one({"flight_count": {"$gt": 0}}):
        print(str(datetime.now()) + ": Flights loaded - " + str(collection.count_documents({})))
        time.sleep(60)
    print("Document trouvé, lancement du modèle !")

    print("Suffisamment de données, lancement du ML predictor !")

    print('Start data retrieval from API')
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
                                'Status_Description', 'Departure_Status_Description', 'Arrival_Status_Description', 'future', 'Arrival_Predicted_Datetime'], axis=1, errors='ignore')
            print(df_minimized.columns)
    except Exception as e:
        print(f"Erreur API {endpoint} : {e}")
    print(df.head())
    print('End data retrieval from API')
    print('Start data cleanup')
        
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

    values = {
        "Departure_weather_rain": df_w_datetime_red['Departure_weather_rain'].median(), 
        "Departure_weather_snowfall": df_w_datetime_red['Departure_weather_snowfall'].median(), 
        "Departure_weather_temperature_2m": df_w_datetime_red['Departure_weather_temperature_2m'].median(),
        "Departure_weather_relative_humidity_2m": df_w_datetime_red['Departure_weather_relative_humidity_2m'].median(), 
        "Departure_weather_wind_speed_100m": df_w_datetime_red['Departure_weather_wind_speed_100m'].median(),
        "Departure_weather_cloud_cover": df_w_datetime_red['Departure_weather_cloud_cover'].median(),
        "Arrival_weather_rain": df_w_datetime_red['Arrival_weather_rain'].median(), 
        "Arrival_weather_snowfall": df_w_datetime_red['Arrival_weather_snowfall'].median(), 
        "Arrival_weather_temperature_2m": df_w_datetime_red['Arrival_weather_temperature_2m'].median(),
        "Arrival_weather_relative_humidity_2m": df_w_datetime_red['Arrival_weather_relative_humidity_2m'].median(), 
        "Arrival_weather_wind_speed_100m": df_w_datetime_red['Arrival_weather_wind_speed_100m'].median(),
        "Arrival_weather_cloud_cover": df_w_datetime_red['Arrival_weather_cloud_cover'].median(),
        "Departure_Actual_Datetime": df_w_datetime_red['Departure_Scheduled_Datetime']
    }

    df_no_na = df_w_datetime_red.fillna(value=values)

    df_final = df_no_na.drop(['Departure_weather_time', 'Arrival_weather_time', '_id', 'is_delayed'], axis=1, errors='ignore')

    def replace_is_delayed(val):
        if val:
            return 1
        else:
            return 0

    df_final['Dep_Sched_Hour'] = df_final['Departure_Scheduled_Datetime'].dt.hour
    df_final['Dep_Sched_Day'] = df_final['Departure_Scheduled_Datetime'].dt.day
    df_final['Dep_Sched_Month'] = df_final['Departure_Scheduled_Datetime'].dt.month
    df_final['Dep_Sched_Weekday'] = df_final['Departure_Scheduled_Datetime'].dt.weekday

    df_final['Arr_Sched_Hour'] = df_final['Arrival_Scheduled_Datetime'].dt.hour
    df_final['Arr_Sched_Day'] = df_final['Arrival_Scheduled_Datetime'].dt.day
    df_final['Arr_Sched_Month'] = df_final['Arrival_Scheduled_Datetime'].dt.month
    df_final['Arr_Sched_Weekday'] = df_final['Arrival_Scheduled_Datetime'].dt.weekday



    df_final_split = df_final.drop(columns=[
        'Departure_Scheduled_Datetime', 'Arrival_Scheduled_Datetime',
        'Departure_Actual_Datetime', 'Arrival_Actual_Datetime'
    ], errors='ignore')

    print('End data cleanup')

    print('Start data preparation for model')

    # Insérez votre code ici
    feats = df_final_split.drop('delay (min)', axis=1, errors='ignore')
    target = df_final_split['delay (min)']
    # Insérez votre code ici
    X_train, X_test, y_train, y_test = train_test_split(feats, target, test_size=0.25, random_state=42)

    cat_cols=['Departure_AirportCode', 
            'Departure_Status_Code', 'Arrival_AirportCode', 
            'Arrival_Status_Code', 'OperatingCarrier_AirlineID', 
            'OperatingCarrier_FlightNumber', 'Equipment_AircraftCode', 
            'Status_Code']

    # OneHotEncoder
    ohe = OneHotEncoder(drop="first", sparse=False, handle_unknown='ignore')

    # Fit-transform sur le train
    X_train_encoded = pd.DataFrame(
        ohe.fit_transform(X_train[cat_cols]),
        columns=ohe.get_feature_names_out(cat_cols),
        index=X_train.index
    )

    # Transform sur le test
    X_test_encoded = pd.DataFrame(
        ohe.transform(X_test[cat_cols]),
        columns=ohe.get_feature_names_out(cat_cols),
        index=X_test.index
    )

    # Supprimer les colonnes originales catégorielles
    X_train = X_train.drop(cat_cols, axis=1)
    X_test = X_test.drop(cat_cols, axis=1)

    # Concaténer les colonnes encodées
    X_train = pd.concat([X_train, X_train_encoded], axis=1)
    X_test = pd.concat([X_test, X_test_encoded], axis=1)

    cols = X_train.columns

    scaler = StandardScaler()

    X_train[cols] = scaler.fit_transform(X_train[cols])

    X_test[cols] = scaler.transform(X_test[cols])

    print('End of data preparation for model')

    print('Start of model training')

    model = RandomForestRegressor(n_estimators=200, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    print("MAE :", mean_absolute_error(y_test, y_pred))
    print("RMSE :", np.sqrt(mean_squared_error(y_test, y_pred)))
    print("R² :", r2_score(y_test, y_pred))

    print("Score Train :", model.score(X_train, y_train))
    print("Score test :", model.score(X_test, y_test))

    print('Start prediction data cleanup')

    API_BASE_URL = os.getenv("API_BASE_URL", "http://api:8000")  # Utiliser l'IP externe si configuré
    endpoint = '/flights-all-future'

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
                                'Status_Description', 'Departure_Status_Description', 'Arrival_Status_Description', 'future', 'Arrival_Predicted_Datetime'], axis=1, errors='ignore')
            print(df_minimized.columns)
            #print(df_minimized['future'])
    except Exception as e:
        print(f"Erreur API {endpoint} : {e}")
        
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

    values = {
        "Departure_weather_rain": df_w_datetime_red['Departure_weather_rain'].median(), 
        "Departure_weather_snowfall": df_w_datetime_red['Departure_weather_snowfall'].median(), 
        "Departure_weather_temperature_2m": df_w_datetime_red['Departure_weather_temperature_2m'].median(),
        "Departure_weather_relative_humidity_2m": df_w_datetime_red['Departure_weather_relative_humidity_2m'].median(), 
        "Departure_weather_wind_speed_100m": df_w_datetime_red['Departure_weather_wind_speed_100m'].median(),
        "Departure_weather_cloud_cover": df_w_datetime_red['Departure_weather_cloud_cover'].median(),
        "Arrival_weather_rain": df_w_datetime_red['Arrival_weather_rain'].median(), 
        "Arrival_weather_snowfall": df_w_datetime_red['Arrival_weather_snowfall'].median(), 
        "Arrival_weather_temperature_2m": df_w_datetime_red['Arrival_weather_temperature_2m'].median(),
        "Arrival_weather_relative_humidity_2m": df_w_datetime_red['Arrival_weather_relative_humidity_2m'].median(), 
        "Arrival_weather_wind_speed_100m": df_w_datetime_red['Arrival_weather_wind_speed_100m'].median(),
        "Arrival_weather_cloud_cover": df_w_datetime_red['Arrival_weather_cloud_cover'].median(),
        "Departure_Actual_Datetime": df_w_datetime_red['Departure_Scheduled_Datetime']
    }

    df_no_na = df_w_datetime_red.fillna(value=values)
    #print(df_no_na.isna().sum())

    df_final = df_no_na.drop(['Departure_weather_time', 'Arrival_weather_time', '_id', 'is_delayed'], axis=1, errors='ignore')

    def replace_is_delayed(val):
        if val:
            return 1
        else:
            return 0

    #df_final['is_delayed'] = df_final['is_delayed'].apply(replace_is_delayed)

    print(df_final)

    # Exemple : extraire heure, jour, mois, jour de semaine
    df_final['Dep_Sched_Hour'] = df_final['Departure_Scheduled_Datetime'].dt.hour
    df_final['Dep_Sched_Day'] = df_final['Departure_Scheduled_Datetime'].dt.day
    df_final['Dep_Sched_Month'] = df_final['Departure_Scheduled_Datetime'].dt.month
    df_final['Dep_Sched_Weekday'] = df_final['Departure_Scheduled_Datetime'].dt.weekday

    df_final['Arr_Sched_Hour'] = df_final['Arrival_Scheduled_Datetime'].dt.hour
    df_final['Arr_Sched_Day'] = df_final['Arrival_Scheduled_Datetime'].dt.day
    df_final['Arr_Sched_Month'] = df_final['Arrival_Scheduled_Datetime'].dt.month
    df_final['Arr_Sched_Weekday'] = df_final['Arrival_Scheduled_Datetime'].dt.weekday

    df_final_split = df_final.drop(columns=[
        'Departure_Scheduled_Datetime', 'Arrival_Scheduled_Datetime',
        'Departure_Actual_Datetime', 'Arrival_Actual_Datetime'
    ], axis=1, errors='ignore')

    print('End prediction data cleanup')
    print('Start prediction future data')

    # Insérez votre code ici
    feats = df_final_split.drop('delay (min)', axis=1)
    target = df_final_split['delay (min)']

    X_future_encoded = pd.DataFrame(
        ohe.transform(feats[cat_cols]),
        columns=ohe.get_feature_names_out(cat_cols),
        index=feats.index
    )

    feats = feats.drop(cat_cols, axis=1)
    feats = pd.concat([feats, X_future_encoded], axis=1)

    feats[cols] = scaler.transform(feats[cols])

    y_new_pred = model.predict(feats)
    df_final_split_predicted = df_w_datetime
    df_final_split_predicted['Arrival_Predicted_Datetime'] = y_new_pred

    df_final_split_predicted['Arrival_Predicted_Datetime'] = df_final_split_predicted['Arrival_Scheduled_Datetime'] + pd.to_timedelta(df_final_split_predicted['Arrival_Predicted_Datetime'], unit='m')

    result_json = df_final_split_predicted.to_dict(orient='records')
    json_reconstructed = [unflatten(rec, separator='_') for rec in result_json]

    print('End prediction future data')

    print('start update predicted time in db')

    for _, row in df_final_split_predicted.iterrows():
        collection.update_one(
            {"_id": ObjectId(row["_id"])},   # Identifie le document
            {"$set": {
                'Arrival_Predicted_Datetime': row['Arrival_Predicted_Datetime']
                }
            }  # Ajoute ou met à jour
        )

    return json_reconstructed

if __name__ == "__main__":
    main()



