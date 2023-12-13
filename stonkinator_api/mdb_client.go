package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	// "go.mongodb.org/mongo-driver/mongo/readpref"
)

var mdb *mongo.Database
// Define collection variables here instead of locally in functions
// where they are used?

const MARKET_LISTS_COLLECTION string = "market_lists"
const INSTRUMENTS_COLLECTION string = "instruments"
const TRADING_SYSTEMS_COLLECTION string = "systems"
const MARKET_STATES_COLLECTION string = "market_states"
const POSITIONS_COLLECTION string = "positions"
const SINGLE_SYMBOL_POS_COLLECTION string = "single_symbol_positions"

func initMdb() {
	// connectionString := fmt.Sprintf(
	// 	"%s://%s:%s@%s:%s",
	// 	os.Getenv("MDB_SERVICE"),
	// 	os.Getenv("MDB_USER"),
	// 	os.Getenv("MDB_PASSWORD"),
	// 	os.Getenv("MDB_SERVICE"),
	// 	os.Getenv("MDB_PORT"),
	// )
	connectionString := fmt.Sprintf("%s", os.Getenv("ATLAS_MONGO_DB_URL"))

	clientOptions := options.Client().ApplyURI(connectionString)
	client, err := mongo.Connect(context.Background(), clientOptions)

	if err != nil {
		log.Fatal("Error connecting to the Mongo database")
		panic(err)
	}

	// defer client.Disconnect(context.Background())

	// Get DB name from a defined env var
	mdb = client.Database("client_db")

	// fmt.Println(mdb.Client().Ping(context.Background(), readpref.Primary()))
	// defer client.Disconnect(context.Background())
	// fmt.Println(mdb.Client().Ping(context.Background(), readpref.Primary()))
}

func GetMdb() (*mongo.Database, error) {
	if mdb == nil {
		return nil, errors.New("mdb has not been initialised")
	} else {
		return mdb, nil
	}
}
