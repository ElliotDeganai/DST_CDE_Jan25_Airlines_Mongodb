#!/usr/bin/env python3
"""
API principale
"""
from fastapi import FastAPI, HTTPException, Depends, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
#from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Text, Boolean, text
#from sqlalchemy.ext.declarative import declarative_base
#from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel, Field
from datetime import datetime, timedelta, date, time
from typing import List, Optional, Dict, Any
from flatten_json import flatten, unflatten
import requests
import os
import logging
from contextlib import contextmanager
#import ./Pipeline/db_connection as mongodb_connection
from pymongo import MongoClient, ASCENDING, DESCENDING
import pandas as pd
import numpy as np

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


# logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, "main.log")),
        logging.StreamHandler()  # Still prints to stdout for Docker logs
    ]
)

logger = logging.getLogger(__name__)

# Configuration de la base de données
""" DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@db:5432/flight_delays")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base() """

mongodb_client = client_dst

# Configuration FastAPI
app = FastAPI(
    title="Flight Delay Prediction API",
    description="API pour les prédictions de retards de vols en France",
    version="1.0.0"
)

# CORS pour permettre les requêtes du frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Modèles Pydantic pour les réponses API
class Coordinate(BaseModel):
    Latitude: float
    Longitude: float

class Position(BaseModel):
    Coordinate: Coordinate

class Name(BaseModel):
    LanguageCode: str
    Value: str

class Airport(BaseModel):
    AirportCode: str
    Position: Position
    CityCode: str
    CountryCode: str
    LocationType: str
    Names: List[Name]
    UtcOffset: str
    TimeZoneId: str

    @classmethod
    def from_dict(cls, data: Dict):
        raw_names = data["Names"]["Name"]
        if isinstance(raw_names, dict):
            raw_names = [raw_names]
        return cls(
            AirportCode=data["AirportCode"],
            Position=Position(Coordinate=Coordinate(**data["Position"]["Coordinate"])),
            CityCode=data["CityCode"],
            CountryCode=data["CountryCode"],
            LocationType=data["LocationType"],
            Names=[Name(LanguageCode=n["@LanguageCode"], Value=n["$"]) for n in raw_names],
            UtcOffset=data["UtcOffset"],
            TimeZoneId=data["TimeZoneId"]
        )

class Scheduled(BaseModel):
    Date: Optional[date]
    Time: Optional[time]

class Actual(BaseModel):
    Date: Optional[date]
    Time: Optional[time]

class Terminal(BaseModel):
    Gate: Optional[str]

class Status(BaseModel):
    Code: Optional[str]
    Description: Optional[str]

class OperatingCarrier(BaseModel):
    AirlineID: Optional[str]
    FlightNumber: Optional[str]

class Equipment(BaseModel):
    AircraftCode: Optional[str]

class Departure(BaseModel):
    AirportCode: Optional[str]
    Scheduled: Optional[Scheduled]
    Actual: Optional[Actual]
    Terminal: Optional[Terminal]
    Status: Optional[Status]

class Arrival(BaseModel):
    AirportCode: Optional[str]
    Scheduled: Optional[Scheduled]
    Actual: Optional[Actual]
    Terminal: Optional[Terminal]
    Status: Optional[Status]


class Flight(BaseModel):
    Departure: Departure
    Arrival: Arrival
    OperatingCarrier: OperatingCarrier
    Equipment: Equipment
    Status: Status

    @classmethod
    def from_dict(cls, data: Dict):

        # Fonction utilitaire pour récupérer une valeur ou un dict vide
        def safe_get(d: dict, key: str, default=None):
            val = d.get(key, default)
            if val is None:
                return default
            return val

        dep_data = safe_get(data, "Departure", {})
        arr_data = safe_get(data, "Arrival", {})

        return cls(
            Departure=Departure(
                AirportCode=safe_get(dep_data, "AirportCode", "UNKNOWN"),
                Scheduled=Scheduled(**safe_get(dep_data, "Scheduled", {})) if dep_data.get("Scheduled") else None,
                Actual=Actual(**safe_get(dep_data, "Actual", {})) if dep_data.get("Actual") else None,
                Terminal=Terminal(**safe_get(dep_data, "Terminal", {})) if dep_data.get("Terminal") else None,
                Status=Status(**safe_get(dep_data, "Status", {})) if dep_data.get("Status") else None,
            ),
            Arrival=Arrival(
                AirportCode=safe_get(arr_data, "AirportCode", "UNKNOWN"),
                Scheduled=Scheduled(**safe_get(arr_data, "Scheduled", {})) if arr_data.get("Scheduled") else None,
                Actual=Actual(**safe_get(arr_data, "Actual", {})) if arr_data.get("Actual") else None,
                Terminal=Terminal(**safe_get(arr_data, "Terminal", {"Gate": "No Gate"})) if arr_data.get("Terminal") else {"Gate": "No Gate"},
                Status=Status(**safe_get(arr_data, "Status", {})) if arr_data.get("Status") else None,
            ),
            OperatingCarrier=OperatingCarrier(**safe_get(data, "OperatingCarrier", {})),
            Equipment=Equipment(**safe_get(data, "Equipment", {})),
            Status=Status(**safe_get(data, "Status", {}))
        )


class FlightPrediction(BaseModel):
    predicted_delay_minutes: Optional[int]
    delay_probability: Optional[float]
    confidence_score: Optional[float]
    model_version: Optional[str]


class FlightQuery(BaseModel):
    flight_number: Optional[str] = None
    departure_date: Optional[datetime] = None
    departure_airport: Optional[str] = None
    arrival_airport: Optional[str] = None
    airline: Optional[str] = None


class DelayPredictionRequest(BaseModel):
    flight_number: str
    departure_date: datetime
    departure_airport: str
    arrival_airport: str


# Dépendance pour obtenir la session de bdd
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def serialize_doc(doc):
    """Convertit ObjectId -> str"""
    doc["_id"] = str(doc["_id"])
    return doc

@app.get("/")  # ok
async def root():
    return {"message": "airlines_project_main_API", "version": "1.0.0"}


@app.get("/health")  # ok
async def health_check():
    """
    Vérification de la santé de l'API
    """
    # Test simple de connexion à la base
    try:
        mongodb_client.list_database_names()
        return {"status": "healthy", "timestamp": datetime.now()}

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unavailable")


@app.get("/airports")  # ok
async def get_airports():
    """
    Récupérer la liste des aéroports européens
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    airport_collection = db["airport"]
    docs = airport_collection.find({})
    return [serialize_doc(doc) for doc in docs]


@app.get("/airlines")  # ok
async def get_airlines():
    """
    Récupérer la liste des compagnies aériennes
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    airline_collection = db["airline"]
    docs = airline_collection.find({})
    return [serialize_doc(doc) for doc in docs]


@app.get("/airports-by-code")  # ok
async def get_airports(
    airport_code: str
):
    """
    Récupérer la liste des aéroports européens
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    airport_collection = db["airport"]
    docs = airport_collection.find({"AirportCode": airport_code}).limit(1)
    return [serialize_doc(doc) for doc in docs]


@app.get("/flights")  # ok
async def get_flights(
    limit: int = Query(50, le=100),
    offset: int = Query(0, ge=0)    
):
    """
    Récupérer la liste des vols
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    flight_collection = db["flight"]
    docs = flight_collection.find({}).limit(limit)
    return [serialize_doc(doc) for doc in docs]


@app.get("/flights-count-past")  # ok
async def get_flights_count_past(   
):
    """
    Récupérer la liste des vols
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    flight_collection = db["flight"]
    return flight_collection.count_documents({
    "$or": [
        {"future": 0},
        {"future": {"$exists": False}}
    ]})


@app.get("/flights-count-future")  # ok
async def get_flights_count_future(   
):
    """
    Récupérer la liste des vols
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    flight_collection = db["flight"]
    return flight_collection.count_documents({"future": 1})


@app.get("/flights-count-future")  # ok
async def get_flights_count_future(   
):
    """
    Récupérer la liste des vols
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    flight_collection = db["flight"]
    return flight_collection.count_documents({})

@app.get("/flights-all")  # ok
async def get_flights_all():
    """
    Récupérer tous les vols
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    flight_collection = db["flight"]
    docs = flight_collection.find({})
    return [serialize_doc(doc) for doc in docs]

@app.get("/flights-all-past")  # ok
async def get_flights_all_past():
    """
    Récupérer tous les vols
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    flight_collection = db["flight"]
    docs = flight_collection.find({
    "$or": [
        {"future": 0},
        {"future": {"$exists": False}}
    ]})
    return [serialize_doc(doc) for doc in docs]

@app.get("/flights-all-future")  # ok
async def get_flights_all_future():
    """
    Récupérer tous les vols
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    flight_collection = db["flight"]
    docs = flight_collection.find({"future": 1})
    return [serialize_doc(doc) for doc in docs]

@app.get("/flights-future")  # ok
async def get_flights_future(
    limit: int = Query(50, le=100),
    offset: int = Query(0, ge=0)
    ):
    """
    Récupérer tous les vols
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    flight_collection = db["flight"]
    docs = flight_collection.find({"future": 1}).sort([
        ("Departure.Scheduled.Date", DESCENDING),
        ("Departure.Scheduled.Time", DESCENDING)
    ]).skip(offset).limit(limit)
    return [serialize_doc(doc) for doc in docs]

@app.get("/flights-past")  # ok
async def get_flights_past(
    limit: int = Query(50, le=100),
    offset: int = Query(0, ge=0) 
    ):
    """
    Récupérer tous les vols
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    flight_collection = db["flight"]
    docs = flight_collection.find({
    "$or": [
        {"future": 0},
        {"future": {"$exists": False}}
    ]}).sort([
        ("Departure.Scheduled.Date", DESCENDING),
        ("Departure.Scheduled.Time", DESCENDING)
    ]).skip(offset).limit(limit)
    return [serialize_doc(doc) for doc in docs]

@app.get("/flights-stats")  # ok
async def get_flights_stats( 
    ):
    """
    Récupérer les statistiques
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    stats_collection = db["statistics"]
    docs = stats_collection.find({}).limit(1)
    return [serialize_doc(doc) for doc in docs]

@app.get("/flights-stats-airlines")  # ok
async def get_flights_stats_airlines( 
    ):
    """
    Récupérer les statistiques
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    stats_collection = db["statistics_by_airline"]
    docs = stats_collection.find({})
    return [serialize_doc(doc) for doc in docs]

@app.get("/flights-stats-departure-airport")  # ok
async def get_flights_stats_departure_airport( 
    ):
    """
    Récupérer les statistiques
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    stats_collection = db["statistics_by_airport_dep"]
    docs = stats_collection.find({})
    return [serialize_doc(doc) for doc in docs]

@app.get("/flights-stats-arrival-airport")  # ok
async def get_flights_stats_arrival_airport( 
    ):
    """
    Récupérer les statistiques
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    stats_collection = db["statistics_by_airport_arr"]
    docs = stats_collection.find({})
    return [serialize_doc(doc) for doc in docs]

@app.get("/cities")  # ok
async def get_cities(
    limit: int = Query(50, le=100),
    offset: int = Query(0, ge=0)    
):
    """
    Récupérer la liste des villes
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    city_collection = db["city"]
    docs = city_collection.find({}).limit(limit)
    return [serialize_doc(doc) for doc in docs]

@app.get("/countries")  # ok
async def get_countries(
    limit: int = Query(50, le=100),
    offset: int = Query(0, ge=0)    
):
    """
    Récupérer la liste des pays
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    country_collection = db["country"]
    docs = country_collection.find({}).limit(limit)
    return [serialize_doc(doc) for doc in docs]

@app.get("/weathers")  # ok
async def get_weathers(
    limit: int = Query(50, le=100),
    offset: int = Query(0, ge=0)    
):
    """
    Récupérer la données de meteo
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    weather_collection = db["weather"]
    docs = weather_collection.find({}).limit(limit)
    return [serialize_doc(doc) for doc in docs]

@app.get("/weathers-airport")  # ok
async def get_weathers_by_airport(
    airport_code: str
):
    """
    Récupérer la données de meteo
    """
    db = mongodb_client["sample"]  # <-- adapte selon le nom de ta base
    weather_collection = db["weather"]
    docs = weather_collection.find({"AirportCode": airport_code})
    return [serialize_doc(doc) for doc in docs]


# Lancement du script si appelé directement
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
