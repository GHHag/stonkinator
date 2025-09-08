#!/bin/bash

protoc \
        --proto_path=./protobufs \
        --go_out=./rpc_service/src \
        --go-grpc_out=./rpc_service/src \
        general_messages.proto \
        securities_service.proto \
        trading_systems_service.proto \
        data_frame_service.proto

OUT_DIR="./stonkinator/stonkinator/persistance/persistance_services"

python3 -m grpc_tools.protoc \
        -I./protobufs \
        --python_out="${OUT_DIR}" \
        --grpc_python_out="${OUT_DIR}" \
        ./protobufs/general_messages.proto \
        ./protobufs/securities_service.proto \
        ./protobufs/trading_systems_service.proto

sed -i 's|import general_messages_pb2|from persistance.persistance_services import general_messages_pb2|' "${OUT_DIR}/securities_service_pb2_grpc.py"
sed -i 's|import general_messages_pb2|import persistance.persistance_services.general_messages_pb2|' "${OUT_DIR}/securities_service_pb2.py"
sed -i 's|import securities_service_pb2|from persistance.persistance_services import securities_service_pb2|' "${OUT_DIR}/securities_service_pb2_grpc.py"

sed -i 's|import general_messages_pb2|from persistance.persistance_services import general_messages_pb2|' "${OUT_DIR}/trading_systems_service_pb2_grpc.py"
sed -i 's|import general_messages_pb2|import persistance.persistance_services.general_messages_pb2|' "${OUT_DIR}/trading_systems_service_pb2.py"
sed -i 's|import trading_systems_service_pb2|from persistance.persistance_services import trading_systems_service_pb2|' "${OUT_DIR}/trading_systems_service_pb2_grpc.py"

OUT_DIR="./stonkinator/stonkinator/data_frame"

python3 -m grpc_tools.protoc \
        -I./protobufs \
        --python_out="${OUT_DIR}" \
        --grpc_python_out="${OUT_DIR}" \
        ./protobufs/data_frame_service.proto

sed -i 's|import general_messages_pb2|from persistance.persistance_services import general_messages_pb2|' "${OUT_DIR}/data_frame_service_pb2_grpc.py"
sed -i 's|import general_messages_pb2|import persistance.persistance_services.general_messages_pb2|' "${OUT_DIR}/data_frame_service_pb2.py"
sed -i 's|import securities_service_pb2|from persistance.persistance_services import securities_service_pb2|' "${OUT_DIR}/data_frame_service_pb2_grpc.py"
sed -i 's|import securities_service_pb2|import persistance.persistance_services.securities_service_pb2|' "${OUT_DIR}/data_frame_service_pb2.py"