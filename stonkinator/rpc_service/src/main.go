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

	service := service{
		infoLog:  log.New(os.Stdout, "INFO\t", log.Ldate|log.Ltime),
		errorLog: log.New(os.Stderr, "Error\t", log.Ldate|log.Ltime),
	}
	service.create(pgPool, "/etc/ssl/rpc_service.pem", "/etc/ssl/private/rpc_service.key", "/etc/ssl/ca.pem")
	if err := service.run(port); err != nil {
		panic(err)
	}
}
