package main

import (
	"context"
	"encoding/json"
	"net/http"
	"go.mongodb.org/mongo-driver/bson"
	// "go.mongodb.org/mongo-driver/mongo"
)

const ID_FIELD string = "_id"
const SYSTEM_ID_FIELD = "system_id"
const SYMBOL_FIELD = "symbol"
const SYSTEM_NAME_FIELD = "name"
const METRICS_FIELD = "metrics"
const POSITION_LIST_FIELD = "position_list_json"

// /systems
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

// /systems/metrics/:id
func getSystemMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// collection := mdb.Collection(TRADING_SYSTEMS_COLLECTION)
}

// /systems/positions
// /systems/positions/:systemId
func systemPositionsAction(w http.ResponseWriter, r *http.Request) {
	getSystemPositions(w, r)
	getSystemPositionsForSymbol(w, r)
}

func getSystemPositions(w http.ResponseWriter, r *http.Request) {

}

func getSystemPositionsForSymbol(w http.ResponseWriter, r *http.Request) {

}

// /systems/market-states/:systemId
func getSystemMarketStates(w http.ResponseWriter, r *http.Request) {

}

// /systems/market-state
func getSystemMarketStateForSymbol(w http.ResponseWriter, r *http.Request) {

}
