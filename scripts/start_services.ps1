docker compose build
docker compose --profile producer build producer
docker compose up -d zookeeper kafka spark mlflow
docker ps
