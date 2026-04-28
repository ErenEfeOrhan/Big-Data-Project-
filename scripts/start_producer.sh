#!/bin/bash

docker compose --profile producer up -d producer
docker logs spotify_producer --tail 30
