package main

import (
	"encoding/json"
	"net/http"
)

type Exchange struct {
	Id string `json:"id"`
	ExchangeName string `json:"exchange_name"`
	Currency string `json:"currency"`
}

func exchangeAction(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
		case http.MethodGet:

		case http.MethodPost:
			var exchange Exchange
			err := json.NewDecoder(r.Body).Decode(&exchange)
			if err != nil {
				http.Error(w, "Invalid request body", http.StatusBadRequest)
				return
			}
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func insertExchange(exchange Exchange, w http.ResponseWriter, r *http.Request) {
	dbConn, dbContext, err := PgClient()
	if err != nil {
		http.Error(w, "Error connecting to the database", http.StatusInternalServerError)
		return
	}

	query := dbConn.QueryRow(
		dbContext, 
		`
			INSERT INTO exchanges(name, currency)
			VALUES($1, $2)
			ON CONFLICT DO NOTHING
		`, exchange.ExchangeName, exchange.Currency,
	)
	var result string
	err = query.Scan(&result)
	if result == "" {
		http.Error(w, "Failed to insert exchange", http.StatusConflict)
		return
	}
	if err != nil {
		http.Error(w, "Error while inserting into the database", http.StatusInternalServerError)
		return
	}
}

func getExchange(id string, w http.ResponseWriter, r *http.Request) {

}
