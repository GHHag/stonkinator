#!/bin/bash

cp .env ./stonkinator/stonkinator/trading_systems/env.py 
cp .env ./stonkinator/stonkinator/persistance/securities_db_py_dal/env.py
cp .env ./stonkinator/.env
cp .env ./stonkinator_api/.env

if [[ $1 == "watch" ]]; then
    docker compose -f ./docker-compose.yml watch
else
    docker compose -f ./docker-compose.yml up -d
fi