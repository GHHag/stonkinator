#!/bin/bash

# the specified python binary path /usr/local/bin/python is customized 
# for the python bullseye Docker image

if [ -f .env ]; then
    while IFS='=' read -r key value; do
        [[ $key == \#* ]] && continue

        key=$(echo "$key" | tr -d '[:space:]' | tr -d "'")
        value=$(echo "$value" | tr -d '[:space:]' | tr -d "'")

        if [ -n "$key" ] && [ -n "$value" ]; then
            export "$key"="$value"
        else
            echo "$0 - Empty key or value, skipping line."
        fi
    done < .env
else
    echo "$0 - Error: .env file not found"
    exit 1
fi

# TODO: default to some directory if DAL_LOG_FILE_PATH and DAL_LOG_FILE_PATH_CRITICAL
# variables are not found
[[ -n "$DAL_LOG_FILE_PATH" && ! -d "$DAL_LOG_FILE_PATH" ]] && mkdir "$DAL_LOG_FILE_PATH"
[[ -n "$DAL_LOG_FILE_PATH_CRITICAL" && ! -d "$DAL_LOG_FILE_PATH_CRITICAL" ]] && mkdir "$DAL_LOG_FILE_PATH_CRITICAL"

/usr/local/bin/python /app/securities_db_py_dal/securities_db_py_dal/dal.py

if [ -n "$TS_HANDLER_DIR_TARGET" ] && [ -n "$LIVE_SYSTEMS_RELATIVE_DIR" ]; then
    cd "$TS_HANDLER_DIR_TARGET"
    /usr/local/bin/python trading_system_handler.py --trading-systems-dir="$LIVE_SYSTEMS_RELATIVE_DIR" --full-run
else
    echo "$0 - Error: Missing values for TS_HANDLER_DIR_TARGET or LIVE_SYSTEMS_RELATIVE_DIR variables."
fi
