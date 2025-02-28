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
		infoLog: log.New(os.Stdout, "INFO\t", log.Ldate|log.Ltime),
		// TODO: Stderr or stdout here
		// errorLog: log.New(os.Stderr, "Error\t", log.Ldate|log.Ltime),
		errorLog: log.New(os.Stdout, "Error\t", log.Ldate|log.Ltime),
	}
	service.create(pgPool)
	if err := service.run(port); err != nil {
		panic(err)
	}
}
