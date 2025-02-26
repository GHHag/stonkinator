package main

import (
	"bufio"
	"log"
	"os"
	"strings"
)

func main() {
	file, err := os.Open("../../.env")
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

	service := service{
		infoLog: log.New(os.Stdout, "INFO\t", log.Ldate|log.Ltime),
		// TODO: Stderr or stdout here
		// errorLog: log.New(os.Stderr, "Error\t", log.Ldate|log.Ltime),
		errorLog: log.New(os.Stdout, "Error\t", log.Ldate|log.Ltime),
	}
	service.create(pgPool)
	if err := service.run(); err != nil {
		panic(err)
	}
}
