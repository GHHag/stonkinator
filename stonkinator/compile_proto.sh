#!/bin/bash

protoc --go_out=./rpc_service/src --go-grpc_out=./rpc_service/src stonkinator.proto

OUT_DIR="./stonkinator/persistance/persistance_services"

python3 -m grpc_tools.protoc \
        -I. \
        --python_out="${OUT_DIR}" \
        --grpc_python_out="${OUT_DIR}" \
        stonkinator.proto

sed -i 's|import stonkinator_pb2|from persistance.persistance_services import stonkinator_pb2|' "${OUT_DIR}/stonkinator_pb2_grpc.py"
