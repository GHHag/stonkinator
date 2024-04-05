package main

import (
	"context"
	"encoding/json"
	"net/http"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const ID_FIELD string = "_id"
const SYSTEM_ID_FIELD string = "system_id"
const SYMBOL_FIELD string = "symbol"
const SYSTEM_NAME_FIELD string = "name"
const METRICS_FIELD string = "metrics"
const POSITION_LIST_FIELD string = "position_list_json"

func getSystems(mdb *mongo.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		collection := mdb.Collection(TRADING_SYSTEMS_COLLECTION)

		var results []bson.M
		// var results []TradingSystem - define TradingSystem struct?
		cursor, err := collection.Find(context.Background(), bson.M{})
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
}

func getSystemMetrics(mdb *mongo.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id := r.URL.Query().Get("id")
		objId, err := primitive.ObjectIDFromHex(id)
		if err != nil {
			http.Error(w, "Incorrect ID", http.StatusBadRequest)
			return
		}

		collection := mdb.Collection(TRADING_SYSTEMS_COLLECTION)

		var result bson.M
		err = collection.FindOne(
			context.Background(),
			bson.M{ID_FIELD: objId},
			options.FindOne().SetProjection(bson.M{METRICS_FIELD: 1, SYSTEM_NAME_FIELD: 1}),
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
}

func systemPositionsAction(mdb *mongo.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id := r.URL.Query().Get("id")
		symbol := r.URL.Query().Get("symbol")

		if len(symbol) > 0 && len(id) > 0 {
			getSystemPositionsForSymbol(id, symbol, w, mdb)
		} else if len(id) > 0 {
			getSystemPositions(id, w, mdb)
		} else {
			http.Error(w, "Bad request URL query parameters", http.StatusBadRequest)
			return
		}
	}
}

func getSystemPositions(systemId string, w http.ResponseWriter, mdb *mongo.Database) {
	objId, err := primitive.ObjectIDFromHex(systemId)
	if err != nil {
		http.Error(w, "Incorrect ID", http.StatusBadRequest)
		return
	}

	collection := mdb.Collection(POSITIONS_COLLECTION)

	var result bson.M
	err = collection.FindOne(
		context.Background(),
		bson.M{SYSTEM_ID_FIELD: objId},
		options.FindOne().SetProjection(bson.M{POSITION_LIST_FIELD: 1, SYSTEM_NAME_FIELD: 1}),
	).Decode(&result)
	if err == mongo.ErrNoDocuments {
		http.Error(w, "No documents found", http.StatusNoContent)
		return
	} else if err != nil {
		http.Error(w, "Failed to execute query", http.StatusNoContent)
		return
	}

	jsonSystemPositions, err := json.Marshal(result)
	if err != nil {
		http.Error(w, "Failed to marshal data", http.StatusNoContent)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonSystemPositions)
}

func getSystemPositionsForSymbol(systemId string, symbol string, w http.ResponseWriter, mdb *mongo.Database) {
	objId, err := primitive.ObjectIDFromHex(systemId)
	if err != nil {
		http.Error(w, "Incorrect ID", http.StatusBadRequest)
		return
	}

	collection := mdb.Collection(SINGLE_SYMBOL_POS_COLLECTION)

	var result bson.M
	err = collection.FindOne(
		context.Background(),
		bson.M{SYSTEM_ID_FIELD: objId, SYMBOL_FIELD: symbol},
		options.FindOne().SetProjection(bson.M{POSITION_LIST_FIELD: 1, SYSTEM_NAME_FIELD: 1, SYMBOL_FIELD: 1}),
	).Decode(&result)
	if err == mongo.ErrNoDocuments {
		http.Error(w, "No documents found", http.StatusNoContent)
		return
	} else if err != nil {
		http.Error(w, "Failed to execute query", http.StatusNoContent)
		return
	}

	jsonSystemPositions, err := json.Marshal(result)
	if err != nil {
		http.Error(w, "Failed to marshal data", http.StatusNoContent)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonSystemPositions)
}

func getSystemMarketStates(mdb *mongo.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id := r.URL.Query().Get("id")
		objId, err := primitive.ObjectIDFromHex(id)
		if err != nil {
			http.Error(w, "Incorrect ID", http.StatusBadRequest)
			return
		}

		collection := mdb.Collection(MARKET_STATES_COLLECTION)

		var results []bson.M
		cursor, err := collection.Find(context.Background(), bson.M{SYSTEM_ID_FIELD: objId})
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

		jsonMarketStates, err := json.Marshal(results)
		if err != nil {
			http.Error(w, "Failed to marshal data", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonMarketStates)
	}
}

func getSystemMarketStateForSymbol(mdb *mongo.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id := r.URL.Query().Get("id")
		objId, err := primitive.ObjectIDFromHex(id)
		if err != nil {
			http.Error(w, "Incorrect ID", http.StatusBadRequest)
			return
		}
		symbol := r.URL.Query().Get("symbol")

		collection := mdb.Collection(MARKET_STATES_COLLECTION)

		var result bson.M
		err = collection.FindOne(
			context.Background(),
			bson.M{SYSTEM_ID_FIELD: objId, SYMBOL_FIELD: symbol},
		).Decode(&result)
		if err == mongo.ErrNoDocuments {
			http.Error(w, "No documents found", http.StatusNoContent)
			return
		} else if err != nil {
			http.Error(w, "Failed to execute query", http.StatusNoContent)
			return
		}

		jsonMarketState, err := json.Marshal(result)
		if err != nil {
			http.Error(w, "Failed to marshal data", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonMarketState)
	}
}
