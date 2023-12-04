package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"fmt"
)

type Exchange struct {
	Id string `json:"id,omitempty"`
	ExchangeName string `json:"exchange_name"`
	Currency string `json:"currency"`
}

func exchangeAction(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
		case http.MethodGet:
			exchangeName := r.URL.Query().Get("exchange")

			getExchange(exchangeName, w, r)

		case http.MethodPost:
			var exchange Exchange
			err := json.NewDecoder(r.Body).Decode(&exchange)
			if err != nil {
				http.Error(w, "Invalid request body", http.StatusBadRequest)
				return
			} else {
				insertExchange(exchange, w, r)
			}

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func getExchange(exchangeName string, w http.ResponseWriter, r *http.Request) {
	query := pgPool.QueryRow(
		context.Background(),
		`
			SELECT id, exchange_name, currency
			FROM exchanges
			WHERE UPPER(exchange_name) = $1
		`, 
		strings.ToUpper(exchangeName),
	)

	var exchange Exchange
	err := query.Scan(&exchange.Id, &exchange.ExchangeName, &exchange.Currency)
	if err != nil {
		http.Error(w, "Failed to get exchange", http.StatusNoContent)
		return
	}

	jsonExchange, err := json.Marshal(exchange)
	if err != nil {
		http.Error(w, "Failed to marshal exchange object", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonExchange)
}

func insertExchange(exchange Exchange, w http.ResponseWriter, r *http.Request) {
	result, err := pgPool.Exec(
		context.Background(),
		`
			INSERT INTO exchanges(exchange_name, currency)
			VALUES($1, $2)
			ON CONFLICT DO NOTHING
		`, 
		exchange.ExchangeName, exchange.Currency,
	)
	if err != nil {
		http.Error(w, "Error while inserting into the database", http.StatusInternalServerError)
		return
	}
	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		http.Error(w, "Failed to insert exchange", http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, rowsAffected)
}
