package main

import (
	"bufio"
	"log"
	"os"
	"strings"
)

func main() {
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

	initPgPool()
	initMdb()

	// port := os.Getenv("TET_API_PORT")
	port := "5800"
	api_url := os.Getenv("API_URL")

	register(port, api_url)
}
