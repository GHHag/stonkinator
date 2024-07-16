#!/bin/bash

cp .env ./stonkinator/stonkinator/trading_systems/trading_system_development/trading_systems/env.py 
cp .env ./stonkinator/stonkinator/persistance/securities_db_py_dal/env.py
cp .env ./stonkinator/.env
cp .env ./stonkinator_api/.env

docker compose -f ./docker-compose.yml up -d
