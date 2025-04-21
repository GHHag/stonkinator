package main

import (
	"log"
	"os"
)

func main() {
	pgPool, err := initPgPool()
	if err != nil {
		panic(err)
	}

	port := os.Getenv("RPC_SERVICE_PORT")
	certFile := os.Getenv("RPC_SERVER_CERT_PATH")
	keyFile := os.Getenv("RPC_SERVER_KEY_PATH")
	caFile := os.Getenv("RPC_SERVICE_CA_CERT_PATH")

	service := service{
		infoLog:  log.New(os.Stdout, "INFO\t", log.Ldate|log.Ltime),
		errorLog: log.New(os.Stderr, "Error\t", log.Ldate|log.Ltime),
	}
	err = service.create(pgPool, certFile, keyFile, caFile)
	if err != nil {
		panic(err)
	}
	if err = service.run(port); err != nil {
		panic(err)
	}
}
