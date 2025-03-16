#!/bin/bash

protoc --go_out=./rpc_service/src --go-grpc_out=./rpc_service/src ./protobufs/securities_service.proto

OUT_DIR="./stonkinator/persistance/persistance_services"

python3 -m grpc_tools.protoc \
        -I./protobufs \
        --python_out="${OUT_DIR}" \
        --grpc_python_out="${OUT_DIR}" \
        ./protobufs/securities_service.proto

sed -i 's|import securities_service_pb2|from persistance.persistance_services import securities_service_pb2|' "${OUT_DIR}/securities_service_pb2_grpc.py"
