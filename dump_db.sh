#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
if [ -f $DIR/.env ]; then
    while IFS='=' read -r key value; do
        [[ $key == \#* ]] && continue

        key=$(echo "$key" | tr -d '[:space:]' | tr -d "'")
        value=$(echo "$value" | tr -d '[:space:]' | tr -d "'")

        if [ -n "$key" ] && [ -n "$value" ]; then
            export "$key"="$value"
        fi
    done < $DIR/.env
else
    echo "$0 - Error: .env file not found"
    exit 1
fi

vars=("PG_DB_SERVICE" "PG_DB_USER" "PG_DB_NAME")
for var in "${vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo "Error: $var is not set or has no value"
        exit 1
    fi
done

docker exec $(docker ps -aqf ancestor=$PG_DB_SERVICE) sh -c "pg_dump -U $PG_DB_USER $PG_DB_NAME >> /tmp/$PG_DB_NAME.sql"

docker cp $(docker ps -aqf ancestor=$PG_DB_SERVICE):/tmp/$PG_DB_NAME.sql $DIR/stonkinator_api/db_dumps/