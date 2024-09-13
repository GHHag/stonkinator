#!/bin/bash

echo "Running $0"

# TODO: Add help option

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

run_dal=false
full_run=""
retain_history=""
print_data=""
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --run-dal)
            run_dal=true
        ;;
        --full-run)
            full_run="--full-run"
        ;;
        --retain-history)
            retain_history="--retain-history"
        ;;
        --print-data)
            print_data="--print-data"
    esac
    shift
done

if [ "$run_dal" = true ]; then
    echo -e "\n--run-dal: $run_dal\nRunning dal"
    /usr/local/bin/python /app/stonkinator/persistance/securities_db_py_dal/dal.py
fi

if [ -n "$TS_HANDLER_DIR_TARGET" ]; then
    cd "$TS_HANDLER_DIR_TARGET"
    echo -e "\nRunning trading systems\n"
    /usr/local/bin/python trading_system_handler.py $full_run $retain_history $print_data
else
    echo "$0 - Error: Missing value for TS_HANDLER_DIR_TARGET."
fi
