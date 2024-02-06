#!/bin/bash

# the specified python binary path /usr/local/bin/python is customized 
# for the python bullseye Docker image

/usr/local/bin/python /app/securities_db_py_dal/securities_db_py_dal/dal.py

if [ -f .env ]; then
    while IFS='=' read -r key value; do
        if [[ $key == \#* ]]; then
            continue
        fi

        key=$(echo "$key" | tr -d '[:space:]' | tr -d "'")
        value=$(echo "$value" | tr -d '[:space:]' | tr -d "'")

        if [ -n "$key" ] && [ -n "$value" ]; then
            export "$key"="$value"
        else
            echo "run.sh - Empty key or value, skipping line."
        fi
    done < .env
else
    echo "run.sh - Error: .env file not found"
    exit 1
fi

cd "$TS_HANDLER_DIR_TARGET"
/usr/local/bin/python trading_system_handler.py --trading-systems-dir="$LIVE_SYSTEMS_RELATIVE_DIR"
