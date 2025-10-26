"""
Collecte des données sur les APIs Lufthansa et OpenWeatherMap
"""
import asyncio
import aiohttp
import asyncpg
import logging
import os
import time
import base64
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import json

# logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, "data_collector.log")),
        logging.StreamHandler()  # Still prints to stdout for Docker logs
    ]
)

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Gestionnaire de limitation de taux pour respecter les limites API
    (boucle sur les timestamps)
    """
    # Guillaume :
    # J'ai baissé les limites pour ne jamais risquer de les toucher :
    # 4 au lieu de 5 requêtes/seconde et 900 requêtes/heure au lieu de 1000
    def __init__(
        self, max_requests_per_second: int = 4,
        max_requests_per_hour: int = 900
    ):
        self.max_per_second = max_requests_per_second
        self.max_per_hour = max_requests_per_hour
        self.requests_this_second = []
        self.requests_this_hour = []


    async def wait_if_needed(self):
        """
        Mise en attente
        """
        now = time.time()

        # Nettoyer les anciens timestamps
        self.requests_this_second = [t for t in self.requests_this_second if now - t < 1]
        self.requests_this_hour = [t for t in self.requests_this_hour if now - t < 3600]

        # Vérifier les limites (par seconde)
        if len(self.requests_this_second) >= self.max_per_second:
            sleep_time = 1 - (now - self.requests_this_second[0])
            if sleep_time > 0:
                logger.warning(f"Limite par seconde atteinte, attente de {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)

        # Vérifier les limites (par heure)
        if len(self.requests_this_hour) >= self.max_per_hour:
            sleep_time = 3600 - (now - self.requests_this_hour[0])
            if sleep_time > 0:
                logger.warning(f"Limite horaire atteinte, attente de {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)

        # Nouveaux timestamps
        now = time.time()
        self.requests_this_second.append(now)
        self.requests_this_hour.append(now)


class LufthansaAPI:
    """
    Client pour l'API Lufthansa

    Ref  : https://developer.lufthansa.com/docs/read/api_basics/Building_a_Request
    Test : https://developer.lufthansa.com/io-docs

    Flight Status :
    GET /operations/flightstatus/{flightNumber}/{date}
                                 2 letter IATA airline code + flight number
                                               yyyy-MM-dd
    Customer Flight Information :
    GET /operations/customerflightinformation/{flightNumber}/{date}
                                              2 letter IATA airline code + flight number
                                                             yyyy-MM-dd

    """
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.token_expires_at = 0
        self.rate_limiter = RateLimiter()
        self.base_url = "https://api.lufthansa.com/v1"


    # ok... tain, c'était bien lourd, ça : gaffe à ne pas trop le bousculer lui
    async def get_access_token(self, session: aiohttp.ClientSession) -> str:
        """
        Obtenir un token d'accès OAuth2
        """
        if self.access_token and time.time() < self.token_expires_at:
            return self.access_token

        # Encoder les credentials
        credentials = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()

        headers = {
            'Authorization': f'Basic {credentials}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        data = {'grant_type': 'client_credentials'}  # J'ai paumé deux jours de ma vie là-dessus.

        try:
            async with session.post(
                'https://api.lufthansa.com/v1/oauth/token',
                headers=headers,
                data=data
            ) as response:
                if response.status == 200:
                    token_data = await response.json()
                    self.access_token = token_data['access_token']
                    # Guillaume : Le token expire généralement en 1 heure : on se cale à 55 minutes pour être sûr
                    self.token_expires_at = time.time() + token_data.get('expires_in', 3600) - 300
                    logger.info(f"Token d'accès Lufthansa obtenu : {self.access_token}")
                    return self.access_token
                else:
                    logger.error(f"Erreur d'obtention du token Lufthansa : {response.status}")
                    return None

        except Exception as e:
            logger.error(f"Exception lors de l'obtention du token Lufthansa : {e}")
            return None


    async def get_flights(self, session: aiohttp.ClientSession, airport_code: str, date: str) -> List[Dict]:
        """
        Récupérer les vols pour un aéroport et une date donnés
        """
        # Respect des rate limits
        await self.rate_limiter.wait_if_needed()

        # Récupération du token Lufthansa
        token = await self.get_access_token(session)
        if not token:
            return []

        # Headers de la requête incluant l'authorisation
        headers = {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json'
        }

        # Récupérer les départs et arrivées
        flights = []

        for flight_type in ['departures', 'arrivals']:
            # (doc) https://developer.lufthansa.com/docs/read/api_details/operations/Customer_Flight_Information_at_Departure_Airport
            url = f"{self.base_url}/operations/customerflightinformation/{flight_type}/{airport_code}/{date}"

            try:
                async with session.get(url, headers=headers) as response:
                    data = await response.json()
                    if response.status == 200:
                        # data = await response.json()
                        if 'FlightInformation' in data and 'Flights' in data['FlightInformation']:
                            flight_data = data['FlightInformation']['Flights']['Flight']
                            if isinstance(flight_data, dict):
                                flight_data = [flight_data]

                            for flight in flight_data:
                                flights.append({
                                    'type': flight_type,
                                    'data': flight
                                })
                    # Cette $é'($! d'API renvoie une erreur 404 si aucune donnée n'est trouvée
                    else:
                        description = (
                            data.get("ProcessingErrors", {})
                            .get("ProcessingError", {})
                            .get("Description")
                        )
                        if description:
                            logger.warning(f"Erreur de l'API Lufthansa pour {flight_type} : '{description}'")
                        else:
                            logger.warning(f"Erreur de l'API Lufthansa pour {flight_type} : '{response.status}'")

            except Exception as e:
                logger.error(f"Exception lors de la récupération des {flight_type} : {e}")

        return flights


class OpenWeatherMapAPI:
    """
    Client pour l'API OpenWeatherMap
    """

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.openweathermap.org/data/2.5"


    async def get_weather(self, session: aiohttp.ClientSession, lat: float, lon: float) -> Optional[Dict]:
        """
        Récupérer les données météo actuelles
        """
        url = f"{self.base_url}/weather"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.api_key,
            'units': 'metric',  # Celsius
            'lang': 'fr'
        }

        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning(f"Erreur API OpenWeatherMap: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Exception lors de la récupération météo: {e}")
            return None


    async def get_forecast(self, session: aiohttp.ClientSession, lat: float, lon: float) -> Optional[Dict]:
        """
        Récupérer les prévisions météo 5 jours
        """
        url = f"{self.base_url}/forecast"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.api_key,
            'units': 'metric',
            'lang': 'fr'
        }

        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning(f"Erreur API OpenWeatherMap forecast: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Exception lors de la récupération des prévisions: {e}")
            return None


class DataCollector:
    """
    Collecteur principal de données
    """
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL", "postgresql://user:password@db:5432/flight_delays")

        # Configuration APIs
        # Key et secret dans le .env, sinon ceux de Guillaume par défaut
        self.lufthansa = LufthansaAPI(
            os.getenv("LUFTHANSA_CLIENT_ID", "fk676v86arf4vdwex5szces3k"),
            os.getenv("LUFTHANSA_CLIENT_SECRET", "YxKB7mNgju")
        )

        self.openweather = OpenWeatherMapAPI(
            os.getenv("OPENWEATHER_API_KEY", "04efb25b47f9a5954c7fc417b4a94176")
        )

        # Codes IATA des aéroports à surveiller
        # Codés en dur, mais prévus dans le .env -> à linker ?
        self.airports_list = [
            'CDG', 'ORY', 'NCE', 'LYS', 'MRS', 'TLS', 'BOD', 'NTE', 'SXB', 'LIL'
        ]


    async def get_db_connection(self):
        """
        Connexion à la bdd
        """
        return await asyncpg.connect(self.db_url)


    async def get_airport_coordinates(self, conn: asyncpg.Connection) -> Dict[str, tuple]:
        """
        Récupérer les coordonnées des aéroports
        pour interroger OpenWeatherMap
        """
        query = "SELECT iata_code, latitude, longitude FROM airports"
        rows = await conn.fetch(query)
        return {row['iata_code']: (row['latitude'], row['longitude']) for row in rows}


    async def save_airline_if_not_exists(self, conn: asyncpg.Connection, airline_code: str, airline_name: str = None):
        """
        Sauvegarder une compagnie aérienne si elle n'existe pas
        """
        query = """
            INSERT INTO airlines (iata_code, name)
            VALUES ($1, $2)
            ON CONFLICT (iata_code) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
        """
        result = await conn.fetchrow(query, airline_code, airline_name or airline_code)
        return result['id']


    async def save_flight_data(self, conn: asyncpg.Connection, flight_data: Dict, flight_type: str):
        """
        Sauvegarder les données de vol
        """
        try:
            # Extraire les informations de base
            flight_info = flight_data['data']

            # flight_number = flight_info.get('FlightNumber', {}).get('FlightNumber', '')
            # flight_number = flight_info.get("data", {}).get("Flight", {}).get("OperatingCarrier", {}).get("FlightNumber", "")
            # airline_code = flight_info.get('FlightNumber', {}).get('AirlineID', '')
            operating_carrier = flight_info.get("OperatingCarrier", {})
            flight_number = operating_carrier.get("FlightNumber", "")
            airline_code = operating_carrier.get("AirlineID", "")

            if not flight_number or not airline_code:
                logger.warning("Absence de flight_number et/ou de airline_code")
                return

            # Sauvegarder la compagnie aérienne
            airline_id = await self.save_airline_if_not_exists(conn, airline_code)

            # Horaires
            scheduled_time = None
            actual_time = None

            # Départs
            if flight_type == 'departures':
                airport_code = flight_info.get("Departure", {}).get("AirportCode", {})
                scheduled_info = flight_info.get("Departure", {}).get("Scheduled", {})
                scheduled_dt = datetime.strptime(f"{scheduled_info.get('Date', '')} {scheduled_info.get('Time', '')}", "%Y-%m-%d %H:%M")
                actual_info = flight_info.get("Departure", {}).get("Actual", {})
                actual_dt = datetime.strptime(f"{actual_info.get('Date', '')} {actual_info.get('Time', '')}", "%Y-%m-%d %H:%M")

            # Arrivées
            else:
                airport_code = flight_info.get("Arrival", {}).get("AirportCode", {})
                scheduled_info = flight_info.get("Arrival", {}).get("Scheduled", {})
                scheduled_dt = datetime.strptime(f"{scheduled_info.get('Date', '')} {scheduled_info.get('Time', '')}", "%Y-%m-%d %H:%M")
                actual_info = flight_info.get("Arrival", {}).get("Actual", {})
                actual_dt = datetime.strptime(f"{actual_info.get('Date', '')} {actual_info.get('Time', '')}", "%Y-%m-%d %H:%M")


            if not scheduled_dt or not airport_code:
                return

            # Calculer le retard
            delay_minutes = 0
            if actual_dt and scheduled_dt:
                delay_minutes = int((actual_dt - scheduled_dt).total_seconds() / 60)

            # Statut du vol
            status = flight_info.get('Status', {}).get('Code', 'SCHEDULED')

            # Récupérer les IDs des aéroports
            if flight_type == 'departures':
                dep_airport_query = "SELECT id FROM airports WHERE iata_code = $1"
                dep_airport_row = await conn.fetchrow(dep_airport_query, airport_code)
                if not dep_airport_row:
                    return
                dep_airport_id = dep_airport_row['id']
                arr_airport_id = None  # Pas d'info sur l'arrivée dans les départs Lufthansa
            else:
                arr_airport_query = "SELECT id FROM airports WHERE iata_code = $1"
                arr_airport_row = await conn.fetchrow(arr_airport_query, airport_code)
                if not arr_airport_row:
                    return
                arr_airport_id = arr_airport_row['id']
                dep_airport_id = None  # Pas d'info sur le départ dans les arrivées Lufthansa

            # Si on a des infos partielles, essayer de compléter avec les données existantes
            if dep_airport_id and not arr_airport_id:
                # Chercher un vol existant pour compléter
                existing_query = """
                    SELECT arrival_airport_id FROM flights
                    WHERE flight_number = $1 AND departure_airport_id = $2
                    AND scheduled_departure = $3
                """
                existing = await conn.fetchrow(existing_query, flight_number, dep_airport_id, scheduled_dt)
                if existing:
                    logger.info("ID de l'aéroport d'arrivée trouvée dans les départs (même vol, même heure de départ prévue)")
                    arr_airport_id = existing['arrival_airport_id']

            # Insérer ou mettre à jour le vol
            upsert_query = f"""
                INSERT INTO flights (
                    flight_number, airline_id, departure_airport_id, arrival_airport_id,
                    scheduled_departure, scheduled_arrival, actual_departure, actual_arrival,
                    status, departure_delay_minutes, arrival_delay_minutes,
                    data_source, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                ON CONFLICT (flight_number, scheduled_departure)
                DO UPDATE SET
                    actual_departure = COALESCE(EXCLUDED.actual_departure, flights.actual_departure),
                    actual_arrival = COALESCE(EXCLUDED.actual_arrival, flights.actual_arrival),
                    status = EXCLUDED.status,
                    departure_delay_minutes = CASE
                        WHEN '{flight_type}' = 'departures' THEN EXCLUDED.departure_delay_minutes
                        ELSE flights.departure_delay_minutes
                    END,
                    arrival_delay_minutes = CASE
                        WHEN '{flight_type}' = 'arrivals' THEN EXCLUDED.arrival_delay_minutes
                        ELSE flights.arrival_delay_minutes
                    END,
                    updated_at = EXCLUDED.updated_at
            """

            # Lufthansa ne file pas l'heure d'arrivée sur les départs, ni les heures de départ sur les arrivées... Une autre idée ?
            # Si elle n'existe pas, on crée une heure d'arrivée prévue (départ prévu + 2 heures) pour les vols au départ
            scheduled_arrival = scheduled_dt + timedelta(hours=2) if flight_type == 'departures' else scheduled_dt
            # Si elle n'existe pas, on crée une heure d'arrivée réelle (départ réel + 2 heures) pour les vols au départ
            actual_arrival = actual_dt + timedelta(hours=2) if actual_dt and flight_type == 'departures' else actual_dt

            await conn.execute(
                upsert_query,
                flight_number, airline_id, dep_airport_id, arr_airport_id,
                scheduled_dt, scheduled_arrival,
                actual_dt if flight_type == 'departures' else None,
                actual_dt if flight_type == 'arrivals' else actual_arrival,
                status,
                delay_minutes if flight_type == 'departures' else 0,
                delay_minutes if flight_type == 'arrivals' else 0,
                'LUFTHANSA', datetime.now()
            )

            logger.info(f"Vol {flight_number} sauvegardé ({flight_type})")

        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde du vol: {e}")


    async def save_weather_data(self, conn: asyncpg.Connection, weather_data: Dict, airport_code: str):
        """
        Sauvegarde des données météo
        """
        try:
            # Récupérer l'ID de l'aéroport
            airport_query = "SELECT id FROM airports WHERE iata_code = $1"
            airport_row = await conn.fetchrow(airport_query, airport_code)
            if not airport_row:
                return
            airport_id = airport_row['id']

            weather_time = datetime.fromtimestamp(weather_data['dt'])

            # Extraire les données météo
            main = weather_data.get('main', {})
            weather = weather_data.get('weather', [{}])[0]
            wind = weather_data.get('wind', {})
            rain = weather_data.get('rain', {})
            snow = weather_data.get('snow', {})
            clouds = weather_data.get('clouds', {})

            # Insérer les données météo
            insert_query = """
                INSERT INTO weather_data (
                    airport_id, weather_time, temperature, humidity, pressure,
                    wind_speed, wind_direction, visibility, weather_main, weather_description,
                    cloud_cover, rain_1h, rain_3h, snow_1h, snow_3h, data_source
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                ON CONFLICT (airport_id, weather_time, data_source) DO UPDATE SET
                    temperature = EXCLUDED.temperature,
                    humidity = EXCLUDED.humidity,
                    pressure = EXCLUDED.pressure,
                    wind_speed = EXCLUDED.wind_speed,
                    wind_direction = EXCLUDED.wind_direction,
                    visibility = EXCLUDED.visibility,
                    weather_main = EXCLUDED.weather_main,
                    weather_description = EXCLUDED.weather_description,
                    cloud_cover = EXCLUDED.cloud_cover,
                    rain_1h = EXCLUDED.rain_1h,
                    rain_3h = EXCLUDED.rain_3h,
                    snow_1h = EXCLUDED.snow_1h,
                    snow_3h = EXCLUDED.snow_3h
            """

            await conn.execute(
                insert_query,
                airport_id, weather_time,
                main.get('temp'), main.get('humidity'), main.get('pressure'),
                wind.get('speed'), wind.get('deg'),
                weather_data.get('visibility', 0) / 1000 if weather_data.get('visibility') else None,  # Convertir en km
                weather.get('main'), weather.get('description'),
                clouds.get('all'), rain.get('1h'), rain.get('3h'), snow.get('1h'), snow.get('3h'),
                'OPENWEATHERMAP'
            )

            logger.info(f"Données météo sauvegardées pour '{airport_code}'")

        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde des données météo: {e}")


    async def collect_flight_data(self, session: aiohttp.ClientSession, conn: asyncpg.Connection):
        """
        Collecter les données de vols
        """
        # L'API Lufthansa limite les résultats aux 4 heures suivantes de la date donnée
        time_slots = ["T00:00", "T04:00", "T08:00", "T12:00", "T16:00", "T20:00"]

        today = datetime.now()
        tomorrow = today + timedelta(days=1)

        for airport in self.airports_list:
            logger.info(f"Collecte des vols pour l'aéroport '{airport}'")

            # Aujourd'hui et demain
            for date in [today, tomorrow]:
                date_str = date.strftime('%Y-%m-%d')

                # Heures de collecte
                for slot in time_slots:
                    datetime_str = f"{date_str}{slot}"
                    logger.info(f"Récupération des vols pour {airport} à {datetime_str}")

                    try:
                        flights = await self.lufthansa.get_flights(session, airport, datetime_str)
                        logger.info(f"Nombre de vols découverts : {len(flights)}")  # debug

                    except Exception as e:
                        logger.warning(f"Erreur lors de la récupération des vols pour {airport} ({datetime_str}) : {e}")
                        continue

                    for flight_data in flights:
                        await self.save_flight_data(conn, flight_data, flight_data['type'])

                    await asyncio.sleep(0.5)  # Trop ? Pas assez ? Ça a l'air de marcher, mais ça me semble beaucoup


    async def collect_weather_data(self, session: aiohttp.ClientSession, conn: asyncpg.Connection):
        """
        Collecter les données météorologiques
        1. On récupère l'id et les coordonnées des aéroports dans la bdd
        2. On interroge OWM avec les coordonnées
        3. On sauvegarde la réponse OWM pour l'id de chaque aéroport dans la bdd
        """
        coordinates = await self.get_airport_coordinates(conn)

        for airport_code, (lat, lon) in coordinates.items():
            if lat is None or lon is None:
                logger.warning(f"Erreur de collecte météo pour {airport_code} : il manque des coordonnées dans la base.")
                continue

            logger.info(f"Collecte météo pour {airport_code}")

            # Données météo actuelles
            weather_data = await self.openweather.get_weather(session, lat, lon)
            if weather_data:
                await self.save_weather_data(conn, weather_data, airport_code)

            await asyncio.sleep(0.1)  # Trop ? Pas assez ? Ça a l'air de marcher, mais ça me semble peu


    async def run_collection_cycle(self):
        """
        Exécuter un cycle complet de collecte
        """
        logger.info("Début du cycle de collecte")

        conn = None
        session = None

        try:
            conn = await self.get_db_connection()
            session = aiohttp.ClientSession()

            # Collecter les données de vol
            await self.collect_flight_data(session, conn)

            # Collecter les données météo
            await self.collect_weather_data(session, conn)

            logger.info("Cycle de collecte terminé")

        except Exception as e:
            logger.error(f"Erreur lors du cycle de collecte: {e}")

        # On est gentils avec la bdd
        finally:
            if session:
                await session.close()
            if conn:
                await conn.close()


    async def start_continuous_collection(self, interval_minutes: int = 30):  # 30 minutes par défaut. Trop ? Pas assez ? linké dans le .env
        """
        Collecte continue
        Lance run_collection_cycle() toutes les 30 minutes
        """
        logger.info(f"Démarrage de la collecte continue (intervalle: {interval_minutes} minutes)")

        while True:
            try:
                await self.run_collection_cycle()
                await asyncio.sleep(interval_minutes * 60)
            except KeyboardInterrupt:
                logger.info("Arrêt de la collecte")
                break
            except Exception as e:
                logger.error(f"Erreur dans la boucle de collecte: {e}")
                await asyncio.sleep(300)  # Attendre 5 minutes en cas d'erreur


# Lancement du script si appelé directement
if __name__ == "__main__":
    collector = DataCollector()
    asyncio.run(collector.start_continuous_collection())
