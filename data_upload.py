import requests
import pprint
import datetime
import json
import time

# Mes identifiants API de Lufthansa
CLIENT_ID = "pjwhfma8qp8ub6phsa6hmjkze"
CLIENT_SECRET = "H5wFdgEYt3"

# URL pour obtenir un jeton d'accès
TOKEN_URL = "https://api.lufthansa.com/v1/oauth/token"
API_BASE = "https://api.lufthansa.com/v1/mds-references"


def get_access_token():
    """
    Récupérer le token de l'API Lufthansa
    """
    payload = {"grant_type": "client_credentials"}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(
        TOKEN_URL,
        auth=(CLIENT_ID, CLIENT_SECRET),
        data=payload,
        headers=headers
    )

    if response.status_code == 200:
        token_info = response.json()
        return token_info["access_token"]
    else:
        print(f"Erreur à l'étape du jeton d'accès (code {response.status_code})")

        # Essayer de décoder en JSON, sinon afficher texte brut
        try:
            print("Réponse JSON :", response.json())
        except Exception:
            print("Réponse brute :", response.text)

        print("Vérifie que ton Client ID et Client Secret sont corrects et actifs.")
        return None


def aeroports_par_pays(pays, headers):
    """
    Obtenir tous les aéroports pour un pays donné
    """
    #loop1=20
    airports = []
    limit = 100
    offset = 0
    while True:
        time.sleep(1)
        print(f"Requête {pays} offset {offset}")
        url = f"{API_BASE}/airports?countryCode={pays}&limit={limit}&offset={offset}"
        print("Selection des aéroports du pays", pays," avec url:", url)
        r = requests.get(url, headers=headers)
        data = r.json()

        if "AirportResource" not in data:
            print("fin des données :")
            # pprint.pprint(airports)
            return airports

        try:
            entries = data["AirportResource"]["Airports"]["Airport"]
        except KeyError:
            print(f"Fin de pagination après offset {offset}.")
            return airports

        if not entries:
            print(f"Plus de résultats après offset {offset}.")
            return airports

        # Filtrage strict sur le CountryCode car on touve des aeroports d'autres pays dans le résultat
        # les données ne sont pas très propres, il faut les cleaner
        for airport in entries:
            if airport.get("CountryCode") == pays:
                airports.append(airport)
                print(airport)
        offset += limit
        #loop1 -= 1
        #if loop1 <1:
        #    break
    return airports

# Code principal
def main():
    # Appel de la fonction pour obtenir le token
    access_token = get_access_token()

    # Vérifie si on a bien reçu un token avant de continuer
    if access_token is None:
        print("Erreur : impossible d'obtenir le jeton d'accès à l'API Lufthansa.")
        exit()

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }

    # Liste des pays européens (codes ISO)
    codes_europe = ["FR", "DE", "IT", "ES", "PT", "NL", "BE", "CH", "AT", "SE", "NO", "FI", "DK", "PL"]

    # On a la liste des 27 ici : https://fr.wikipedia.org/wiki/États_membres_de_l%27Union_européenne
    # On les met tous ?
    # codes_europe = ["AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI",
    #     "FR", "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU",
    #     "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE"]

    result = []

    for pays in codes_europe:
        airports = aeroports_par_pays(pays, headers)
        for airport in airports:
            print(airport)

            # Vérification du nom de l'aéroport
            try:
                name_data = airport.get("Names", {}).get("Name", "")
                if isinstance(name_data, list):
                    airport_name = next((n["$"] for n in name_data if n.get("@LanguageCode") == "EN"), "UNKNOWN")
                    print("1",airport_name)
                elif isinstance(name_data, dict):
                    airport_name = name_data.get("$", "UNKNOWN") if name_data.get("@LanguageCode") == "EN" else "UNKNOWN"
                    print("2",airport_name)
                else:
                    airport_name = str(name_data)
                    print("3",airport_name)

            except Exception as e:
                print("Erreur dans le nom de l'aéroport :", e)
                airport_name = "UNKNOWN"

            result.append({
                "airport_code": airport.get("AirportCode"),
                "airport_name": airport_name,
                "country_code": pays
            })
    # print(result)

    # Sauvegarde dans un fichier JSON avec date
    date_fichier = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    filename = f"data/json/airports_europe_{date_fichier}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"\nDonnées enregistrées dans le fichier suivant : {filename}")


# Lancement du script si appelé directement
if __name__ == "__main__":
    main()
    print("fini")
