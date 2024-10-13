#!/bin/bash

cp .env ./stonkinator/stonkinator/trading_systems/env.py 
cp .env ./stonkinator/stonkinator/persistance/securities_db_py_dal/env.py
cp .env ./stonkinator/.env
cp .env ./stonkinator_api/.env
cp .env ./alphascrape/.env

watch=false
run_cron=false

for argument in "$@"
do
	case $argument in
		--watch)
			watch=true
			;;
		--run-cron)
			run_cron=true
			;;
	esac
done

if $watch; then
    docker compose -f ./docker-compose.yml watch
else
    docker compose -f ./docker-compose.yml up -d
fi

if $run_cron; then
	DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
	CRON_JOB="0 21 1 * * $DIR/dump_db.sh >> /var/log/dump_db.log 2>&1"

	crontab -l | grep -q "$CRON_JOB"
	if [ $? -eq 0 ]; then
	    echo "Cron job already exists."
	else
	    (crontab -l; echo "$CRON_JOB") | crontab -
	    echo "Cron job added."
	fi
fi
