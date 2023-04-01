#!/bin/bash
# source ./daily_data_gather.sh

alias server_path='cd api'
alias dal_path='cd ../securities_db_py_dal/securities_db_py_dal'
alias system_handler_path='cd ../../../tet_trading_systems/tet_trading_systems/trading_system_development/trading_systems'

server_path
node server.js &

dal_path
python dal.py

system_handler_path
python trading_system_handler.py

# kill node processes
pkill -f node