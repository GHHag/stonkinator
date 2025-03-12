#!/bin/bash

client="stonkinator"
server="rpc_service"

if [ -z "$1" ]; then
    exit 1
fi

dir=$1

if [ ! -d "$dir" ]; then
    mkdir -p "$dir"
fi

# The following command will create a CA certificate that can be used to sign a server’s certificate.
# It will output two files:
#     *.key is a private key.
#     *.pem is a public certificate.
openssl req \
    -x509 \
    -nodes \
    -newkey rsa:4096 \
    -keyout $dir/ca.key \
    -out $dir/ca.pem \
    -subj /O=me

# Create a certificate for the server and sign it with the CA certificate.
# This will produce three new files:
#     *.key is the server’s private key.
#     *.csr is an intermediate file.
#     *.pem is the server’s public certificate.
openssl req \
    -nodes \
    -newkey rsa:4096 \
    -keyout $dir/$server.key \
    -out $dir/$server.csr \
    -subj /CN=$server
openssl x509 \
    -req \
    -in $dir/$server.csr \
    -CA $dir/ca.pem \
    -CAkey $dir/ca.key \
    -set_serial 1 \
    -out $dir/$server.pem

# Create a certificate for the client
openssl req \
    -nodes \
    -newkey rsa:4096 \
    -keyout $dir/$client.key \
    -out $dir/$client.csr \
    -subj /CN=$client
openssl x509 \
    -req \
    -in $dir/$client.csr \
    -CA $dir/ca.pem \
    -CAkey $dir/ca.key \
    -set_serial 1 \
    -out $dir/$client.pem