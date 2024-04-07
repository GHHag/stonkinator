package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func initPgPool() *pgxpool.Pool {
	connectionString := fmt.Sprintf(
		"postgresql://%s:%s@%s:%s/%s",
		os.Getenv("PG_DB_USER"),
		os.Getenv("PG_DB_PASSWORD"),
		os.Getenv("PG_DB_SERVICE"),
		os.Getenv("PG_DB_PORT"),
		os.Getenv("PG_DB_NAME"),
	)

	pgPool, err := pgxpool.New(context.Background(), connectionString)
	if err != nil {
		panic(err)
	}

	if pgPool == nil {
		panic(errors.New("pgPool is nil"))
	}

	return pgPool
}
