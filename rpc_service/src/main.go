package main

import (
	"log"
	"os"
)

func main() {
	service := service{
		infoLog: log.New(os.Stdout, "INFO\t", log.Ldate|log.Ltime),
		// TODO: Stderr or stdout here
		// errorLog: log.New(os.Stderr, "Error\t", log.Ldate|log.Ltime),
		errorLog: log.New(os.Stdout, "Error\t", log.Ldate|log.Ltime),
	}
	service.create()
	if err := service.run(); err != nil {
		panic(err)
	}
}
