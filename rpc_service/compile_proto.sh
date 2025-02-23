#!/bin/bash

protoc --go_out=./src --go-grpc_out=./src stonkinator.proto

# Example from https://grpc.io/docs/languages/go/quickstart/
# protoc --go_out=. --go_opt=paths=source_relative \
#     --go-grpc_out=. --go-grpc_opt=paths=source_relative \
#     helloworld/helloworld.proto

python3 -m grpc_tools.protoc -I. --python_out=./src/client --grpc_python_out=./src/client stonkinator.proto
