package main

import (
	"bufio"
	"stonkinator/entities"

	// "flag"
	"log"
	"os"
	"strings"
)

func main() {
	// envFilePath := flag.String("env", ".env", "Path to the .env file")
	// flag.Parse()
	// file, err := os.Open(*envFilePath)
	file, err := os.Open(".env")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.ReplaceAll(line, " ", "")
		line = strings.ReplaceAll(line, "'", "")
		line = strings.ReplaceAll(line, "\"", "")
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key, value := parts[0], parts[1]
			os.Setenv(key, value)
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	pgPool, err := initPgPool()
	if err != nil {
		panic(err)
	}

	apiUrl := os.Getenv("API_URL")
	port := os.Getenv("STONKINATOR_API_PORT")
	// port := os.Getenv("STONKINATOR_API_PORT_EXP") // when running locally

	app := app{
		pgPool:   pgPool,
		infoLog:  log.New(os.Stdout, "INFO\t", log.Ldate|log.Ltime),
		errorLog: log.New(os.Stdout, "ERROR\t", log.Ldate|log.Ltime|log.Lshortfile),
		entities: entities.Entities{},
	}
	app.create(apiUrl)
	// if err = app.run(port, "../stonkify.crt", "../stonkify.key"); err != nil { // when running locally
	if err = app.run(port, "/app/stonkify.crt", "/app/stonkify.key"); err != nil {
		panic(err)
	}
}
