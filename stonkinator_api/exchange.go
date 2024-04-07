package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Exchange struct {
	Id           string `json:"id,omitempty"`
	ExchangeName string `json:"exchange_name"`
	Currency     string `json:"currency"`
}

func exchangeAction(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getExchange(w, r, pgPool)

		case http.MethodPost:
			insertExchange(w, r, pgPool)

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func getExchange(w http.ResponseWriter, r *http.Request, pgPool *pgxpool.Pool) {
	exchangeName := r.URL.Query().Get("exchange")

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

func insertExchange(w http.ResponseWriter, r *http.Request, pgPool *pgxpool.Pool) {
	var exchange Exchange
	err := json.NewDecoder(r.Body).Decode(&exchange)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

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
