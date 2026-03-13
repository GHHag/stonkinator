package main

import (
	"flag"
	"log"
	"os"
	"stonkinator/entities"
)

func main() {
	insecure := flag.Bool("insecure", false, "run without TLS")
	flag.Parse()

	pgPool, err := initPgPool()
	if err != nil {
		panic(err)
	}

	apiUrl := os.Getenv("API_URL")
	port := os.Getenv("WEB_API_PORT")
	certFile := os.Getenv("WEB_API_CERT_PATH")
	keyFile := os.Getenv("WEB_API_KEY_PATH")

	app := app{
		pgPool:   pgPool,
		infoLog:  log.New(os.Stdout, "INFO\t", log.Ldate|log.Ltime),
		errorLog: log.New(os.Stderr, "ERROR\t", log.Ldate|log.Ltime|log.Lshortfile),
		entities: entities.Entities{},
	}
	app.create(apiUrl)

	if *insecure {
		err = app.runInsecure(port)
	} else {
		err = app.run(port, certFile, keyFile)
	}

	if err != nil {
		panic(err)
	}
}
