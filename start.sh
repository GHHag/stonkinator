#!/bin/bash

cp .env ./stonkinator/stonkinator/trading_systems/env.py 
cp .env ./stonkinator/stonkinator/persistance/securities_db_py_dal/env.py
cp .env ./stonkinator/.env
cp .env ./stonkinator_api/.env
cp .env ./alphascrape/.env

if [[ $1 == "watch" ]]; then
    docker compose -f ./docker-compose.yml watch
else
    docker compose -f ./docker-compose.yml up -d
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
[[ ! -d "$DIR/logs" ]] && mkdir "$DIR/logs"
CRON_JOB="0 21 1 * * $DIR/dump_db.sh >> $DIR/logs/dump_db.log 2>&1"

crontab -l | grep -q "$CRON_JOB"
if [ $? -eq 0 ]; then
    echo "Cron job already exists."
else
    (crontab -l; echo "$CRON_JOB") | crontab -
    echo "Cron job added."
fi
