#!/bin/bash

cp .env ./tet_py_packages/tet_trading_systems/tet_trading_systems/trading_system_development/trading_systems/env.py 
cp .env ./tet_py_packages/securities_db_py_dal/securities_db_py_dal/env.py
cp .env ./stonkinator_api/.env

if [[ ! -d ./tet_py_packages/securities_db_py_dal/logs ]]; then
	mkdir ./tet_py_packages/securities_db_py_dal/logs
fi

docker compose -f ./docker-compose.yml up -d
