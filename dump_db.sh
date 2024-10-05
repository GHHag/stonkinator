#!/bin/bash

if [ -f .env ]; then
    while IFS='=' read -r key value; do
        [[ $key == \#* ]] && continue

        key=$(echo "$key" | tr -d '[:space:]' | tr -d "'")
        value=$(echo "$value" | tr -d '[:space:]' | tr -d "'")

        if [ -n "$key" ] && [ -n "$value" ]; then
            export "$key"="$value"
        fi
    done < .env
else
    echo "$0 - Error: .env file not found"
    exit 1
fi

docker exec -it $(docker ps -aqf ancestor=$PG_DB_SERVICE) sh -c "pg_dump -U $PG_DB_USER $PG_DB_NAME >> /tmp/$PG_DB_NAME.sql"

docker cp $(docker ps -aqf ancestor=$PG_DB_SERVICE):/tmp/$PG_DB_NAME.sql ./stonkinator_api/db_dumps/