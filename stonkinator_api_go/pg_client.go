package main

import (
	"context"
	"log"
	"os"
	"fmt"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

var pgPool *pgxpool.Pool

func initPgPool() {
	connectionString := fmt.Sprintf(
		"postgresql://%s:%s@%s:%s/%s",
		os.Getenv("PG_DB_USER"),
		os.Getenv("PG_DB_PASSWORD"),
		os.Getenv("PG_DB_SERVICE"),
		// "0.0.0.0",
		os.Getenv("PG_DB_PORT"),
		// os.Getenv("PG_DB_PORT_EXP"),
		os.Getenv("PG_DB_NAME"),
	)

	var err error
	pgPool, err = pgxpool.New(context.Background(), connectionString)
	if err != nil {
		log.Fatal("Error connecting to the PSQL database: ", err)
		panic(err)
	}

	if pgPool == nil {
		panic(err)
	}
}

func GetPgPool() (*pgxpool.Pool, error) {
	if pgPool == nil {
		return nil, errors.New("pgPool has not been initialised")
	} else {
		return pgPool, nil
	}
}
