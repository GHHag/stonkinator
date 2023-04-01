#!/bin/bash

# the specified python binary path /usr/local/bin/python is customized 
# for the python bullseye Docker image

/usr/local/bin/python /app/securities_db_py_dal/securities_db_py_dal/dal.py

/usr/local/bin/python /app/tet_trading_systems/tet_trading_systems/trading_system_development/trading_systems/trading_system_handler.py
