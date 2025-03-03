#!/bin/bash

echo "Running $0"

# TODO: default to some directory if variables are not found
[[ -n "$DAL_LOG_FILE_PATH" && ! -d "$DAL_LOG_FILE_PATH" ]] && mkdir "$DAL_LOG_FILE_PATH"
[[ -n "$DAL_LOG_FILE_PATH_CRITICAL" && ! -d "$DAL_LOG_FILE_PATH_CRITICAL" ]] && mkdir "$DAL_LOG_FILE_PATH_CRITICAL"
[[ -n "$GRPC_LOG_FILE_PATH" && ! -d "$GRPC_LOG_FILE_PATH" ]] && mkdir "$GRPC_LOG_FILE_PATH"

run_dal=false
full_run=""
retain_history=""
print_data=""
step_through=""
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
        ;;
        --step-through)
            step_through="--step-through"
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
    /usr/local/bin/python trading_system_handler.py $full_run $retain_history $print_data $step_through
else
    echo "$0 - Error: Missing value for TS_HANDLER_DIR_TARGET."
fi
