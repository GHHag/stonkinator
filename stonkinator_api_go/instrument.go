package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
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
func instrumentsAction() {

}

// get /instruments/sector
func getSectorInstruments() {

}

// get /instruments/sectors
func getSectors() {

}

// get /instruments/symbols/:id
func getMarketListInstrumentSymbols() {

}

// get /instruments/sector/market-lists
func getSectorInstrumentsForMarketLists() {

}

// post /market-list
// get /market-list
func marketListAction() {

}

// get /market-lists
func getMarketLists() {

}
