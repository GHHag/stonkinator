package main

import (
	"context"
	"fmt"
	"os"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	// "go.mongodb.org/mongo-driver/mongo/readpref"
)

// Define collection variables here instead of locally in functions
// where they are used?
const MARKET_LISTS_COLLECTION string = "market_lists"
const INSTRUMENTS_COLLECTION string = "instruments"
const TRADING_SYSTEMS_COLLECTION string = "systems"
const MARKET_STATES_COLLECTION string = "market_states"
const POSITIONS_COLLECTION string = "positions"
const SINGLE_SYMBOL_POS_COLLECTION string = "single_symbol_positions"

func initMdb() *mongo.Database {
	connectionString := fmt.Sprintf(
		"%s://%s:%s@%s:%s",
		os.Getenv("MDB_SERVICE"),
		os.Getenv("MDB_USER"),
		os.Getenv("MDB_PASSWORD"),
		os.Getenv("MDB_SERVICE"),
		os.Getenv("MDB_PORT"),
	)
	// connectionString := os.Getenv("ATLAS_MONGO_DB_URL")

	clientOptions := options.Client().ApplyURI(connectionString)
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		panic(err)
	}

	return client.Database(os.Getenv("CLIENT_DB"))
}
