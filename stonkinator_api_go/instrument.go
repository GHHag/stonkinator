package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Instrument struct {
	Id string `json:"id"`
	ExchangeId string `json:"exchange_id"`
	Symbol string `json:"symbol"`
}

func instrumentAction(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
		case http.MethodGet:
			symbol := r.URL.Query().Get("symbol")
			getInstrument(symbol, w, r)

		case http.MethodPost:
			var instrument Instrument
			err := json.NewDecoder(r.Body).Decode(&instrument)
			if err != nil {
				http.Error(w, "Invalid request body", http.StatusBadRequest)
				return
			} else {
				insertInstrument(instrument, w, r)
			}

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func getInstrument(symbol string, w http.ResponseWriter, r *http.Request) {
	query := pgPool.QueryRow(
		context.Background(),
		`
			SELECT id, exchange_id, symbol
			FROM instruments
			WHERE UPPER(symbol) = $1
		`, strings.ToUpper(symbol),
	)

	var instrument Instrument
	err := query.Scan(&instrument.Id, &instrument.ExchangeId, &instrument.Symbol)
	if err != nil {
		http.Error(w, "Failed to get instrument", http.StatusNoContent)
		return
	}

	jsonInstrument, err := json.Marshal(instrument)
	if err != nil {
		http.Error(w, "Failed to marshal instrument object", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonInstrument)
}

func insertInstrument(instrument Instrument, w http.ResponseWriter, r *http.Request) {
	query := pgPool.QueryRow(
		context.Background(),
		`
			INSERT INTO instruments(exchange_id, symbol)
			VALUES($1, $2)
			ON CONFLICT DO NOTHING
		`, instrument.ExchangeId, instrument.Symbol,
	)

	var result string
	err := query.Scan(&result)
	if result == "" {
		http.Error(w, "Failed to insert instrument", http.StatusConflict)
		return
	}
	if err != nil {
		http.Error(w, "Error while inserting into the database", http.StatusInternalServerError)
		return
	}
}

// post /instruments
// get /instruments/:id
func instrumentsAction(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
		case http.MethodGet:
			insertInstruments(w, r)

		case http.MethodPost:
			marketListId := r.URL.Query().Get("id")
			getMarketListInstruments(marketListId, w, r)

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func insertInstruments(w http.ResponseWriter, r *http.Request) {

}

func getMarketListInstruments(marketListId string, w http.ResponseWriter, r *http.Request) {

}

// get /instruments/sector
func getSectorInstruments(w http.ResponseWriter, r *http.Request) {

}

// get /instruments/sectors
func getSectors(w http.ResponseWriter, r *http.Request) {

}

// get /instruments/symbols/:id
func getMarketListInstrumentSymbols(w http.ResponseWriter, r *http.Request) {

}

// get /instruments/sector/market-lists
func getSectorInstrumentsForMarketLists(w http.ResponseWriter, r *http.Request) {

}

// post /market-list
// get /market-list
func marketListAction(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
		case http.MethodGet:
			marketList := r.URL.Query().Get("market-list")
			getMarketListId(marketList, w, r)

		case http.MethodPost:
			insertMarketList(w, r)

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func insertMarketList(w http.ResponseWriter, r *http.Request) {

}

func getMarketListId(marketList string, w http.ResponseWriter, r *http.Request) {
	collection := mdb.Collection(MARKET_LISTS_COLLECTION)

	filter := bson.M{"market_list": marketList}
	projection := bson.M{"_id": 1, "market_list": 0}

	var marketListId string 
	err := collection.FindOne(
		context.Background(), 
		filter, 
		options.FindOne().SetProjection(projection),
	).Decode(&marketListId)
	if err == mongo.ErrNoDocuments {
		http.Error(w, "No documents found", http.StatusNoContent)
		return
	} else if err != nil {
		http.Error(w, "Failed to execute query", http.StatusInternalServerError)
		return
	}

	jsonMarketListId, err := json.Marshal(marketListId)
	if err != nil {
		http.Error(w, "Failed to marshal id", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonMarketListId)
}

// get /market-lists
func getMarketLists(w http.ResponseWriter, r *http.Request) {

}
