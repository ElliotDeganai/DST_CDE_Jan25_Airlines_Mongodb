#!/bin/bash

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# API Lufthansa
# Doc : https://developer.lufthansa.com/docs/API_basics
LUFTHANSA_CLIENT_ID="c2arzvxsb2n23923gv8er9kah"  # ID de Elliot
LUFTHANSA_CLIENT_SECRET="vnHd36JrDG"  # secret de Elliot

# -----------------------------------------------------------------------------
# Configuration facultative :
#  - ces mots de passe ne vous seront pas demandés
#  - les bases sont inaccessibles depuis une autre machine que l'hôte
# -----------------------------------------------------------------------------

# MongoDB (variables non utilisées pour le moment)
MONGO_USER="datascientest"
MONGO_PASSWORD="dst123"
MONGO_HOST="mongodb"
MONGO_PORT=27017


HOST_IP=$(curl -s https://ipinfo.io/ip)

cat > .env << EOF
HOST_IP=$HOST_IP
API_BASE_URL=http://$HOST_IP:8000
EOF

echo "Rappel des infos de connexion :"
echo ""
echo "Dashboard  : http://$HOST_IP:8080"
echo "API Docs   : http://$HOST_IP:8000/docs"