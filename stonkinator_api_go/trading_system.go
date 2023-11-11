package main

import (
	"context"
	"encoding/json"
	"net/http"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const ID_FIELD string = "_id"
const SYSTEM_ID_FIELD = "system_id"
const SYMBOL_FIELD = "symbol"
const SYSTEM_NAME_FIELD = "name"
const METRICS_FIELD = "metrics"
const POSITION_LIST_FIELD = "position_list_json"

func getSystems(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	collection := mdb.Collection(TRADING_SYSTEMS_COLLECTION)

	var results []bson.M
	// var results []TradingSystem
	cursor, err := collection.Find(context.Background(), bson.D{})
	if err != nil {
		http.Error(w, "Failed to execute query", http.StatusInternalServerError)
		return
	}
	defer cursor.Close(context.Background())
	if err = cursor.All(context.Background(), &results); err != nil {
		http.Error(w, "Failed to retrieve result", http.StatusInternalServerError)
		return
	}
	if len(results) == 0 {
		http.Error(w, "No documents found", http.StatusNoContent)
		return
	}

	jsonTradingSystems, err := json.Marshal(results)
	if err != nil {
		http.Error(w, "Failed to marshal data", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonTradingSystems)
}

func getSystemMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := r.URL.Query().Get("id")
	objID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		http.Error(w, "Incorrect ID", http.StatusBadRequest)
		return
	}

	collection := mdb.Collection(TRADING_SYSTEMS_COLLECTION)

	filter := bson.M{ID_FIELD: objID}
	projection := bson.M{METRICS_FIELD: 1, SYSTEM_NAME_FIELD: 1}

	var result bson.M
	err = collection.FindOne(
		context.Background(),
		filter,
		options.FindOne().SetProjection(projection),
	).Decode(&result)
	if err == mongo.ErrNoDocuments {
		http.Error(w, "No documents found", http.StatusNoContent)
		return
	} else if err != nil {
		http.Error(w, "Failed to execute query", http.StatusNoContent)
		return
	}

	jsonSystemMetrics, err := json.Marshal(result)
	if err != nil {
		http.Error(w, "Failed to marshal data", http.StatusNoContent)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonSystemMetrics)
}

// /systems/positions
// /systems/positions/:systemId
func systemPositionsAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := r.URL.Query().Get("id")
	symbol := r.URL.Query().Get("symbol")

	fmt.Println(id)

	if len(symbol) > 0 {
		fmt.Println(symbol)
		getSystemPositionsForSymbol(id, symbol, w, r)
	} else {
		getSystemPositions(id, w, r)
	}
}

func getSystemPositions(systemID string, w http.ResponseWriter, r *http.Request) {
	fmt.Println("getSystemPositions")
}

func getSystemPositionsForSymbol(systemID string, symbol string, w http.ResponseWriter, r *http.Request) {
	fmt.Println("getSystemPositionsForSymbol")
}

// /systems/market-states/:systemId
func getSystemMarketStates(w http.ResponseWriter, r *http.Request) {

}

// /systems/market-state
func getSystemMarketStateForSymbol(w http.ResponseWriter, r *http.Request) {

}
