#!/bin/bash

echo "Ce n'est pas la peine de relancer ce script :"
echo "le projet est déjà configuré pour l'hôte actuel"
echo ""
echo "Ce ne serait utile que si on installait le projet sur un nouvel hôte."
echo ""
echo "Rappel des infos de connexion :"
echo ""
echo "Dashboard  : http://94.130.133.61:8050"
echo "API        : http://94.130.133.61:8000"
echo "API Docs   : http://94.130.133.61:8000/docs"
echo "PostgreSQL : http://94.130.133.61:5432"

exit

HOST_IP=$(hostname -I | awk '{print $1}')
if [ -z "$HOST_IP" ]; then
    HOST_IP=$(ip route get 1.1.1.1 | awk '{print $7; exit}')
fi

# Créer le fichier .env avec la configuration réseau
cat > .env << EOF
# Configuration réseau pour accès distant
HOST_IP=$HOST_IP
API_BASE_URL=http://$HOST_IP:8000

# Configuration base de données
DATABASE_URL=postgresql://user:password@db:5432/flight_delays

# APIs externes
LUFTHANSA_CLIENT_ID=fk676v86arf4vdwex5szces3k
LUFTHANSA_CLIENT_SECRET=YxKB7mNgju
OPENWEATHER_API_KEY=04efb25b47f9a5954c7fc417b4a94176

# Collecte
COLLECTION_INTERVAL_MINUTES=30
EOF

# Mettre à jour docker-compose pour utiliser l'IP externe
cat > docker-compose.override.yml << EOF
version: '3.8'
services:
  dashboard:
    environment:
      API_BASE_URL: http://$HOST_IP:8000
    ports:
      - "0.0.0.0:8050:8050"

  api:
    ports:
      - "0.0.0.0:8000:8000"
EOF

echo "Dashboard : http://$HOST_IP:8050"
echo "API       : http://$HOST_IP:8000"
echo "API Docs  : http://$HOST_IP:8000/docs"
