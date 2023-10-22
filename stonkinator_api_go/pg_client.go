package main

import (
	"context"
	"log"
	"os"
	"github.com/jackc/pgx/v5/pgxpool"
	"fmt"
)

var PgPool *pgxpool.Pool

func init() {
	connectionString := fmt.Sprintf(
		"postgresql://%s:%s@%s:%s/%s",
		os.Getenv("PG_DB_USER"),
		os.Getenv("PG_DB_PASSWORD"),
		os.Getenv("PG_DB_SERVICE"),
		// os.Getenv("PG_DB_PORT"),
		os.Getenv("PG_DB_PORT_EXP"),
		os.Getenv("PG_DB_NAME"),
	)

	var err error
	PgPool, err = pgxpool.New(context.Background(), connectionString)
	if err != nil {
		log.Fatal("Error connecting to the database: ", err)
		panic(err)
	}

	if PgPool == nil {
		panic(err)
	}
}
