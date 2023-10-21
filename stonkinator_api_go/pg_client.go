package main

import (
	"context"
	"log"
	"os"
	"github.com/jackc/pgx/v5/pgxpool"
	"fmt"
)

var PgPool pgxpool.Pool
var Context = context.Background()

func PgClient() (*pgxpool.Pool, context.Context, error) {
	connectionString := fmt.Sprintf(
		"postgresql://%s:%s@%s:%s/%s",
		os.Getenv("PG_DB_USER"),
		os.Getenv("PG_DB_PASSWORD"),
		os.Getenv("PG_DB_SERVICE"),
		os.Getenv("PG_DB_PORT"),
		os.Getenv("PG_DB_NAME"),
	)

	context := context.Background()

	PgPool, err := pgxpool.New(context, connectionString)

	if err != nil {
		log.Fatal("Error connecting to the database: ", err)
		return nil, nil, err
	}

	return PgPool, context, nil
}
