data_upload.py permet de charger les données récupérées de l'API lufthansa dans lefichier json.
- Pour l'instant tous les aéroports d'Europe avec lur nom de pays y ont été chargés
- Structure de données:
  "airport_code": airport.get("AirportCode"),
  "airport_name": airport_name,
  "country_code": pays
-Fichier résultat: airports_europe_2025-07-07.json  (prêt à être uploadé dans une BD)

