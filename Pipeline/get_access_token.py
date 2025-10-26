#!/usr/bin/env python3

import requests
import os
import time
from datetime import datetime, timedelta

def main():
    
    print(str(datetime.now()) + ": Debut de la collection du token lufthansa.")

    url = "https://api.lufthansa.com/v1/oauth/token"

    payload = 'client_id=c2arzvxsb2n23923gv8er9kah&client_secret=vnHd36JrDG&grant_type=client_credentials'
    #payload = 'client_id=adrafmp4t94u4uynjg5wg5kwp&client_secret=RY3Szsjngb&grant_type=client_credentials'
    headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
    }
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        time.sleep(0.5)

        if response.status_code == 200:
            jsonData = response.json()
            if jsonData:
                print(jsonData)
                os.environ['lufthansa_token'] = jsonData["access_token"]
            else:
                print(response.status_code, response.reason, response.text)
    except requests.exceptions.HTTPError as err:
        print(response.status_code)
        print(f"Erreur lors de la requête : {err}")
        data = None  # Gérer l'échec 

    print(str(datetime.now()) + ": Fin de la collection du token lufthansa.")

if __name__ == "__main__":
    main()