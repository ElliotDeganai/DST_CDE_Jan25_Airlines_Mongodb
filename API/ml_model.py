"""
Modèle de machine learning pour la prédiction des retards de vols
"""
import pandas as pd
import numpy as np
from sqlalchemy import text
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import mean_absolute_error, accuracy_score, classification_report
import asyncpg
import joblib
import os
import logging
from datetime import datetime, timedelta
from typing import Tuple, Dict, Any, Optional
import warnings
warnings.filterwarnings('ignore')

# logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, "ml_model.log")),
        logging.StreamHandler()  # Still prints to stdout for Docker logs
    ]
)

logger = logging.getLogger(__name__)


class FlightDelayPredictor:
    """
    Modèle de prédiction des retards de vols
    """

    def __init__(self, db_url: str):
        self.db_url = db_url
        self.delay_regressor = None
        self.delay_classifier = None
        self.scaler = StandardScaler()
        self.label_encoders = {}
        self.feature_columns = []
        self.model_version = "v1.0"
        self.model_path = "/app/models/"

        # Créer le répertoire des modèles
        os.makedirs(self.model_path, exist_ok=True)


    async def fetch_training_data(self) -> pd.DataFrame:
        """
        Récupérer les données d'entraînement depuis la base de données
        """
        conn = await asyncpg.connect(self.db_url)

        try:
            query = """
                SELECT
                    f.id,
                    f.flight_number,
                    f.scheduled_departure,
                    f.actual_departure,
                    f.departure_delay_minutes,
                    f.status,

                    -- Informations sur les aéroports
                    da.iata_code as dep_airport,
                    da.city as dep_city,
                    aa.iata_code as arr_airport,
                    aa.city as arr_city,

                    -- Informations compagnie
                    al.iata_code as airline,
                    al.name as airline_name,

                    -- Données météo au départ
                    wd.temperature,
                    wd.humidity,
                    wd.pressure,
                    wd.wind_speed,
                    wd.wind_direction,
                    wd.visibility,
                    wd.weather_main,
                    wd.weather_description,
                    wd.cloud_cover,
                    wd.rain_1h,
                    wd.rain_3h,
                    wd.snow_1h,
                    wd.snow_3h

                FROM flights f
                LEFT JOIN airports da ON f.departure_airport_id = da.id
                LEFT JOIN airports aa ON f.arrival_airport_id = aa.id
                LEFT JOIN airlines al ON f.airline_id = al.id
                LEFT JOIN weather_data wd ON da.id = wd.airport_id
                    AND wd.weather_time <= f.scheduled_departure
                    AND wd.weather_time > f.scheduled_departure - INTERVAL '2 hours'
                WHERE f.actual_departure IS NOT NULL
                    AND f.departure_delay_minutes IS NOT NULL
                    AND f.scheduled_departure >= CURRENT_DATE - INTERVAL '180 days'
                ORDER BY wd.weather_time DESC
            """

            rows = await conn.fetch(text(query))

            if not rows:
                logger.warning("Aucune donnée d'entraînement trouvée")
                return pd.DataFrame()

            # Convertir en DataFrame
            data = []
            for row in rows:
                data.append(dict(row))

            df = pd.DataFrame(data)
            logger.info(f"Données récupérées: {len(df)} vols")
            return df

        finally:
            await conn.close()


    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prendre en compte des données supp
        """
        df = df.copy()

        # Date / heure
        df['scheduled_departure'] = pd.to_datetime(df['scheduled_departure'])
        df['hour_of_day'] = df['scheduled_departure'].dt.hour
        df['day_of_week'] = df['scheduled_departure'].dt.dayofweek
        df['month'] = df['scheduled_departure'].dt.month
        df['day_of_year'] = df['scheduled_departure'].dt.dayofyear

        # Affluence
        df['is_rush_hour'] = df['hour_of_day'].apply(
            lambda x: 1 if x in [7, 8, 9, 17, 18, 19] else 0  # C'est bien arbitraire... Mais bon...
        )
        df['is_weekend'] = df['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)

        # Météo
        df['temperature'] = df['temperature'].fillna(df['temperature'].median())
        df['humidity'] = df['humidity'].fillna(df['humidity'].median())
        df['wind_speed'] = df['wind_speed'].fillna(0)
        df['visibility'] = df['visibility'].fillna(10)  # Bonne visibilité par défaut
        df['rain_1h'] = df['rain_1h'].fillna(0)
        df['snow_1h'] = df['snow_1h'].fillna(0)
        df['cloud_cover'] = df['cloud_cover'].fillna(0)

        # Météo difficile
        df['bad_weather'] = (
            (df['rain_1h'] > 5) |
            (df['snow_1h'] > 1) |
            (df['wind_speed'] > 50) |
            (df['visibility'] < 1)
        ).astype(int)

        # Sévérité météo
        df['weather_severity'] = (
            df['rain_1h'] * 0.2 +
            df['snow_1h'] * 0.5 +
            (df['wind_speed'] / 10) +
            ((10 - df['visibility']) / 10) +
            (df['cloud_cover'] / 100)
        )

        # Encodage des variables catégorielles
        categorical_features = ['dep_airport', 'arr_airport', 'airline', 'weather_main']

        for feature in categorical_features:
            if feature in df.columns:
                df[feature] = df[feature].fillna('UNKNOWN')
                if feature not in self.label_encoders:
                    self.label_encoders[feature] = LabelEncoder()
                    df[f'{feature}_encoded'] = self.label_encoders[feature].fit_transform(df[feature])
                else:
                    # Pour les nouvelles valeurs non vues pendant l'entraînement
                    known_values = set(self.label_encoders[feature].classes_)
                    df[feature] = df[feature].apply(lambda x: x if x in known_values else 'UNKNOWN')
                    df[f'{feature}_encoded'] = self.label_encoders[feature].transform(df[feature])

        # Statistiques historiques par vol/aéroport/compagnie
        if 'departure_delay_minutes' in df.columns:  # Mode entraînement
            df['airline_avg_delay'] = df.groupby('airline')['departure_delay_minutes'].transform('mean')
            df['airport_avg_delay'] = df.groupby('dep_airport')['departure_delay_minutes'].transform('mean')
            df['route_avg_delay'] = df.groupby(['dep_airport', 'arr_airport'])['departure_delay_minutes'].transform('mean')

        return df


    def prepare_features(self, df: pd.DataFrame, is_training: bool = True) -> pd.DataFrame:
        """
        Préparer les données pour l'entraînement ou la prédiction
        """

        # Données numériques
        numeric_features = [
            'hour_of_day', 'day_of_week', 'month', 'day_of_year',
            'is_rush_hour', 'is_weekend',
            'temperature', 'humidity', 'wind_speed', 'visibility',
            'rain_1h', 'snow_1h', 'cloud_cover', 'bad_weather', 'weather_severity'
        ]

        # Données encodées
        encoded_features = [
            'dep_airport_encoded', 'arr_airport_encoded', 'airline_encoded', 'weather_main_encoded'
        ]

        # Statistiques (entraînement)
        if is_training:
            stat_features = ['airline_avg_delay', 'airport_avg_delay', 'route_avg_delay']
            numeric_features.extend(stat_features)

        # Toutes les données
        all_features = numeric_features + encoded_features
        available_features = [f for f in all_features if f in df.columns]

        if not available_features:
            raise ValueError("Aucune donnée disponible pour l'entraînement")

        self.feature_columns = available_features
        feature_df = df[available_features].copy()

        # Nettoyage NA
        feature_df = feature_df.fillna(0)

        return feature_df


    async def train_model(self) -> Dict[str, Any]:
        """
        Entraîner le modèle de prédiction
        """
        logger.info("Début de l'entraînement du modèle")

        # Récupérer les données
        df = await self.fetch_training_data()
        if df.empty:
            raise ValueError("Pas de données d'entraînement disponibles")

        # Traitement préalable
        df = self.engineer_features(df)

        # Préparation des sets d'entraînement
        X = self.prepare_features(df, is_training=True)

        # Target pour la régression (minutes de retard)
        y_regression = df['departure_delay_minutes'].clip(0, 300)  # Limiter les retards extrêmes

        # Target pour la classification (retard > 15 minutes)
        y_classification = (df['departure_delay_minutes'] > 15).astype(int)

        # Division des données
        X_train, X_test, y_reg_train, y_reg_test, y_cls_train, y_cls_test = train_test_split(
            X, y_regression, y_classification, test_size=0.2, random_state=42
        )

        # Normalisation
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)

        # Entraînement du modèle de régression (prédiction du nombre de minutes)
        logger.info("Entraînement du modèle de régression")
        self.delay_regressor = RandomForestRegressor(
            n_estimators=100,
            max_depth=15,
            min_samples_split=10,
            min_samples_leaf=5,
            random_state=42,
            n_jobs=-1
        )
        self.delay_regressor.fit(X_train_scaled, y_reg_train)

        # Entraînement du modèle de classification (probabilité de retard)
        logger.info("Entraînement du modèle de classification")
        self.delay_classifier = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            min_samples_split=10,
            min_samples_leaf=5,
            random_state=42,
            n_jobs=-1
        )
        self.delay_classifier.fit(X_train_scaled, y_cls_train)

        # Évaluation
        reg_predictions = self.delay_regressor.predict(X_test_scaled)
        cls_predictions = self.delay_classifier.predict(X_test_scaled)
        cls_probabilities = self.delay_classifier.predict_proba(X_test_scaled)[:, 1]

        # Métriques de régression
        mae = mean_absolute_error(y_reg_test, reg_predictions)
        rmse = np.sqrt(np.mean((y_reg_test - reg_predictions) ** 2))

        # Métriques de classification
        accuracy = accuracy_score(y_cls_test, cls_predictions)

        # Importance des features
        feature_importance = dict(zip(
            self.feature_columns,
            self.delay_regressor.feature_importances_
        ))

        # Sauvegarder le modèle
        await self.save_model()

        results = {
            'model_version': self.model_version,
            'training_samples': len(X_train),
            'test_samples': len(X_test),
            'regression_mae': float(mae),
            'regression_rmse': float(rmse),
            'classification_accuracy': float(accuracy),
            'feature_importance': feature_importance,
            'training_date': datetime.now().isoformat()
        }

        logger.info(f"Entraînement terminé - MAE: {mae:.2f} min, Accuracy: {accuracy:.3f}")
        return results


    async def save_model(self):
        """
        Sauvegarder le modèle entraîné
        """
        model_data = {
            'delay_regressor': self.delay_regressor,
            'delay_classifier': self.delay_classifier,
            'scaler': self.scaler,
            'label_encoders': self.label_encoders,
            'feature_columns': self.feature_columns,
            'model_version': self.model_version
        }

        model_file = os.path.join(self.model_path, f'flight_delay_model_{self.model_version}.pkl')
        joblib.dump(model_data, model_file)
        logger.info(f"Modèle sauvegardé: {model_file}")


    async def load_model(self, model_file: str = None):
        """
        Charger un modèle pré-entraîné
        """
        if model_file is None:
            model_file = os.path.join(self.model_path, f'flight_delay_model_{self.model_version}.pkl')

        if not os.path.exists(model_file):
            logger.warning(f"Fichier modèle non trouvé: {model_file}")
            return False

        try:
            model_data = joblib.load(model_file)
            self.delay_regressor = model_data['delay_regressor']
            self.delay_classifier = model_data['delay_classifier']
            self.scaler = model_data['scaler']
            self.label_encoders = model_data['label_encoders']
            self.feature_columns = model_data['feature_columns']
            self.model_version = model_data.get('model_version', 'v1.0')

            logger.info(f"Modèle chargé: {model_file}")
            return True

        except Exception as e:
            logger.error(f"Erreur lors du chargement du modèle: {e}")
            return False


    async def predict_delay(self, flight_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prédire le retard pour un vol donné
        """
        if self.delay_regressor is None or self.delay_classifier is None:
            # Charger le modèle
            if not await self.load_model():
                raise ValueError("Aucun modèle entraîné disponible")

        # DataFrame des données du vol
        df = pd.DataFrame([flight_data])

        # Préparation des données (mode prédiction)
        df['scheduled_departure'] = pd.to_datetime(df['scheduled_departure'])
        df['hour_of_day'] = df['scheduled_departure'].dt.hour
        df['day_of_week'] = df['scheduled_departure'].dt.dayofweek
        df['month'] = df['scheduled_departure'].dt.month
        df['day_of_year'] = df['scheduled_departure'].dt.dayofyear

        # Cas particuliers
        df['is_rush_hour'] = df['hour_of_day'].apply(
            lambda x: 1 if x in [7, 8, 9, 17, 18, 19] else 0
        )
        df['is_weekend'] = df['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)

        # Météo
        weather_features = ['temperature', 'humidity', 'wind_speed', 'visibility',
                           'rain_1h', 'snow_1h', 'cloud_cover', 'weather_main']
        for feature in weather_features:
            if feature not in df.columns:
                # Valeurs par défaut
                defaults = {
                    'temperature': 15, 'humidity': 60, 'wind_speed': 10,
                    'visibility': 10, 'rain_1h': 0, 'snow_1h': 0,
                    'cloud_cover': 30, 'weather_main': 'Clear'
                }
                df[feature] = defaults.get(feature, 0)

        # Météo difficile
        df['bad_weather'] = (
            (df['rain_1h'] > 5) | (df['snow_1h'] > 1) |
            (df['wind_speed'] > 50) | (df['visibility'] < 1)
        ).astype(int)

        df['weather_severity'] = (
            df['rain_1h'] * 0.2 + df['snow_1h'] * 0.5 +
            (df['wind_speed'] / 10) + ((10 - df['visibility']) / 10) +
            (df['cloud_cover'] / 100)
        )

        # Encodage données catégorielles
        categorical_features = ['dep_airport', 'arr_airport', 'airline', 'weather_main']
        for feature in categorical_features:
            if feature in df.columns and feature in self.label_encoders:
                # Valeurs inconnues
                known_values = set(self.label_encoders[feature].classes_)
                df[feature] = df[feature].apply(
                    lambda x: x if x in known_values else 'UNKNOWN'
                )
                df[f'{feature}_encoded'] = self.label_encoders[feature].transform(df[feature])

        # Préparer les données (pas de stats historiques en mode prédiction)
        try:
            feature_df = df[self.feature_columns].copy()
            feature_df = feature_df.fillna(0)

            # Normaliser
            features_scaled = self.scaler.transform(feature_df)

            # Prédictions
            predicted_minutes = max(0, float(self.delay_regressor.predict(features_scaled)[0]))
            delay_probability = float(self.delay_classifier.predict_proba(features_scaled)[0][1])

            # Score de confiance basé sur la cohérence entre les deux modèles
            confidence = 0.8  # Score par défaut
            if predicted_minutes > 15 and delay_probability > 0.5:
                confidence = min(0.95, 0.7 + delay_probability * 0.3)
            elif predicted_minutes < 15 and delay_probability < 0.5:
                confidence = min(0.95, 0.7 + (1 - delay_probability) * 0.3)
            else:
                confidence = 0.6  # Prédictions contradictoires

            return {
                'predicted_delay_minutes': int(predicted_minutes),
                'delay_probability': delay_probability,
                'confidence_score': confidence,
                'model_version': self.model_version
            }

        except Exception as e:
            logger.error(f"Erreur lors de la prédiction: {e}")
            # Prédiction par défaut en cas d'erreur
            return {
                'predicted_delay_minutes': 10,
                'delay_probability': 0.4,
                'confidence_score': 0.3,
                'model_version': self.model_version
            }

    async def retrain_if_needed(self) -> bool:
        """
        Re-entraîner le modèle si nécessaire
        """
        # Vérifier si le modèle existe
        model_file = os.path.join(self.model_path, f'flight_delay_model_{self.model_version}.pkl')

        if not os.path.exists(model_file):
            logger.info("Aucun modèle existant, entraînement nécessaire")
            await self.train_model()
            return True

        # Vérifier l'âge du modèle
        model_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(model_file))

        if model_age.days > 7:  # Re-entraîner chaque semaine
            logger.info(f"Modèle ancien ({model_age.days} jours), re-entraînement")
            await self.train_model()
            return True

        return False


class ModelService:
    """
    Service pour gérer les modèles ML
    """

    def __init__(self, db_url: str):
        self.predictor = FlightDelayPredictor(db_url)


    async def initialize(self):
        """
        Initialiser le service
        """
        await self.predictor.load_model()


    async def predict_flight_delay(self, flight_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prédire le retard d'un vol
        """
        return await self.predictor.predict_delay(flight_data)


    async def retrain_model(self) -> Dict[str, Any]:
        """
        Re-entraîner le modèle
        """
        return await self.predictor.train_model()


    async def save_prediction_to_db(self, flight_id: int, prediction: Dict[str, Any]):
        """
        Sauvegarder une prédiction dans la base de données
        """
        conn = await asyncpg.connect(self.predictor.db_url)

        try:
            query = """
                INSERT INTO flight_predictions (
                    flight_id, predicted_delay_minutes, delay_probability,
                    confidence_score, model_version, features_snapshot
                ) VALUES ($1, $2, $3, $4, $5, $6)
            """

            await conn.execute(
                text(query),
                flight_id,
                prediction['predicted_delay_minutes'],
                prediction['delay_probability'],
                prediction['confidence_score'],
                prediction['model_version'],
                json.dumps(prediction.get('features', {}))
            )

        finally:
            await conn.close()


# Script principal pour l'entraînement
if __name__ == "__main__":
    import asyncio

    async def main():
        db_url = os.getenv("DATABASE_URL", "postgresql://user:password@db:5432/flight_delays")
        predictor = FlightDelayPredictor(db_url)

        # Entraîner le modèle
        results = await predictor.train_model()
        print("Résultats de l'entraînement:")
        for key, value in results.items():
            print(f"  {key}: {value}")

    asyncio.run(main())
