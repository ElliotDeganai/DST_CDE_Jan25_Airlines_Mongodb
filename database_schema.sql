-- Guillaume Un grand merci à Pgadmin qui m'a sorti le schéma d'après mes infâmes bricolages :)

-- Schema de bdd

-- Table des aéroports français
CREATE TABLE airports (
    id SERIAL PRIMARY KEY,
    iata_code VARCHAR(3) UNIQUE NOT NULL,
    icao_code VARCHAR(4),
    name VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(100) DEFAULT 'France',
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    timezone VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des compagnies aériennes
CREATE TABLE airlines (
    id SERIAL PRIMARY KEY,
    iata_code VARCHAR(3) UNIQUE NOT NULL,
    icao_code VARCHAR(4),
    name VARCHAR(255) NOT NULL,
    country VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table principale des vols
CREATE TABLE flights (
    id SERIAL PRIMARY KEY,
    flight_number VARCHAR(10) NOT NULL,
    airline_id INTEGER REFERENCES airlines(id),
    departure_airport_id INTEGER REFERENCES airports(id),
    arrival_airport_id INTEGER REFERENCES airports(id),

    -- Horaires prévus
    scheduled_departure TIMESTAMP NOT NULL,
    scheduled_arrival TIMESTAMP NOT NULL,

    -- Horaires réels (null si pas encore effectué)
    actual_departure TIMESTAMP,
    actual_arrival TIMESTAMP,

    -- Statut du vol
    status VARCHAR(50), -- SCHEDULED, BOARDING, DEPARTED, ARRIVED, CANCELLED, DELAYED

    -- Retards en minutes
    departure_delay_minutes INTEGER DEFAULT 0,
    arrival_delay_minutes INTEGER DEFAULT 0,

    -- Métadonnées
    aircraft_type VARCHAR(50),
    gate VARCHAR(10),
    terminal VARCHAR(10),

    -- Données collectées
    data_source VARCHAR(50) DEFAULT 'LUFTHANSA',
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(flight_number, scheduled_departure)
);

-- Table des données météorologiques
CREATE TABLE weather_data (
    id SERIAL PRIMARY KEY,
    airport_id INTEGER REFERENCES airports(id),

    -- Timestamp de la donnée météo
    weather_time TIMESTAMP NOT NULL,

    -- Données météo principales
    temperature DECIMAL(5, 2), -- Celsius
    humidity INTEGER, -- %
    pressure DECIMAL(7, 2), -- hPa
    wind_speed DECIMAL(5, 2), -- km/h
    wind_direction INTEGER, -- degrees
    visibility DECIMAL(5, 2), -- km

    -- Conditions météo
    weather_main VARCHAR(50), -- Clear, Clouds, Rain, etc.
    weather_description TEXT,
    cloud_cover INTEGER, -- %

    -- Précipitations
    rain_1h DECIMAL(5, 2), -- mm
    rain_3h DECIMAL(5, 2), -- mm
    snow_1h DECIMAL(5, 2), -- mm
    snow_3h DECIMAL(5, 2), -- mm

    -- Source des données
    data_source VARCHAR(50) DEFAULT 'OPENWEATHERMAP',
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(airport_id, weather_time, data_source)
);

-- Table des prédictions du modèle ML
CREATE TABLE flight_predictions (
    id SERIAL PRIMARY KEY,
    flight_id INTEGER REFERENCES flights(id),

    -- Prédiction
    predicted_delay_minutes INTEGER,
    delay_probability DECIMAL(5, 4), -- Probabilité de retard (0-1)
    confidence_score DECIMAL(5, 4), -- Score de confiance (0-1)

    -- Métadonnées du modèle
    model_version VARCHAR(50),
    prediction_made_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Features utilisées pour la prédiction
    features_snapshot JSONB
);

-- Table de logs pour les requêtes API
-- CREATE TABLE api_logs (
--     id SERIAL PRIMARY KEY,
--     api_name VARCHAR(50) NOT NULL, -- LUFTHANSA, OPENWEATHERMAP
--     endpoint VARCHAR(255),
--     request_count INTEGER DEFAULT 1,
--     response_status INTEGER,
--     error_message TEXT,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- Index pour améliorer les performances
CREATE INDEX idx_flights_departure_time ON flights(scheduled_departure);
CREATE INDEX idx_flights_arrival_time ON flights(scheduled_arrival);
CREATE INDEX idx_flights_status ON flights(status);
CREATE INDEX idx_flights_airline ON flights(airline_id);
CREATE INDEX idx_flights_airports ON flights(departure_airport_id, arrival_airport_id);
CREATE INDEX idx_weather_airport_time ON weather_data(airport_id, weather_time);
CREATE INDEX idx_predictions_flight ON flight_predictions(flight_id);
-- CREATE INDEX idx_api_logs_time ON api_logs(created_at);

-- Insertion des principaux aéroports français
INSERT INTO airports (iata_code, icao_code, name, city, latitude, longitude, timezone) VALUES
('CDG', 'LFPG', 'Charles de Gaulle Airport', 'Paris', 49.0097, 2.5479, 'Europe/Paris'),
('ORY', 'LFPO', 'Orly Airport', 'Paris', 48.7233, 2.3794, 'Europe/Paris'),
('NCE', 'LFMN', 'Nice Côte d''Azur Airport', 'Nice', 43.6584, 7.2155, 'Europe/Paris'),
('LYS', 'LFLL', 'Lyon Saint-Exupéry Airport', 'Lyon', 45.7256, 5.0811, 'Europe/Paris'),
('MRS', 'LFML', 'Marseille Provence Airport', 'Marseille', 43.4393, 5.2214, 'Europe/Paris'),
('TLS', 'LFBO', 'Toulouse-Blagnac Airport', 'Toulouse', 43.6291, 1.3638, 'Europe/Paris'),
('BOD', 'LFBD', 'Bordeaux-Mérignac Airport', 'Bordeaux', 44.8283, -0.7156, 'Europe/Paris'),
('NTE', 'LFRS', 'Nantes Atlantique Airport', 'Nantes', 47.1532, -1.6106, 'Europe/Paris'),
('SXB', 'LFLL', 'Strasbourg Airport', 'Strasbourg', 48.5384, 7.6282, 'Europe/Paris'),
('LIL', 'LFQQ', 'Lille Airport', 'Lille', 50.5619, 3.0897, 'Europe/Paris');

-- Vue pour les statistiques des retards
CREATE VIEW flight_delay_stats AS
SELECT
    DATE(scheduled_departure) as flight_date,
    departure_airport_id,
    arrival_airport_id,
    airline_id,
    COUNT(*) as total_flights,
    COUNT(CASE WHEN departure_delay_minutes > 15 THEN 1 END) as delayed_flights,
    AVG(departure_delay_minutes) as avg_delay_minutes,
    MAX(departure_delay_minutes) as max_delay_minutes
FROM flights
WHERE actual_departure IS NOT NULL
GROUP BY DATE(scheduled_departure), departure_airport_id, arrival_airport_id, airline_id;

-- Vue pour les données enrichies des vols avec météo
CREATE VIEW flights_with_weather AS
SELECT
    f.*,
    da.name as departure_airport_name,
    da.city as departure_city,
    aa.name as arrival_airport_name,
    aa.city as arrival_city,
    al.name as airline_name,
    wd.temperature,
    wd.humidity,
    wd.wind_speed,
    wd.weather_main,
    wd.weather_description,
    wd.visibility,
    fp.predicted_delay_minutes,
    fp.delay_probability
FROM flights f
LEFT JOIN airports da ON f.departure_airport_id = da.id
LEFT JOIN airports aa ON f.arrival_airport_id = aa.id
LEFT JOIN airlines al ON f.airline_id = al.id
LEFT JOIN weather_data wd ON da.id = wd.airport_id
    AND wd.weather_time = (
        SELECT MAX(weather_time)
        FROM weather_data wd2
        WHERE wd2.airport_id = da.id
        AND wd2.weather_time <= f.scheduled_departure
    )
LEFT JOIN flight_predictions fp ON f.id = fp.flight_id
    AND fp.id = (
        SELECT MAX(id)
        FROM flight_predictions fp2
        WHERE fp2.flight_id = f.id
    );
