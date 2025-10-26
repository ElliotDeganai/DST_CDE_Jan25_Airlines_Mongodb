#!/bin/bash

echo "Ce script permet de lancer/arrêter les containers Docker"
echo ""
echo ""

read -rsp $'Appuyez sur une touche pour continuer ou CTRL+C pour sortir.\n' -n1 key

if ! command -v docker &> /dev/null; then
    echo "Docker n'est pas installé."
    exit 1
fi

if ! command -v docker compose &> /dev/null; then
    echo "Le plugin Docker 'compose' n'est pas installé."
    exit 1
fi

echo "Lancement de install.sh avant démarrage…"
bash install.sh 

docker stop $(docker ps -a -q)
docker rm $(docker ps -a -q)
docker rmi -f $(docker images -aq)
docker system prune -f
docker-compose build --no-cache
docker compose up -d
