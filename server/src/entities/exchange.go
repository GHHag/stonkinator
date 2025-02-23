package entities

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Exchange struct {
	Id           string `json:"id,omitempty"`
	ExchangeName string `json:"exchange-name"`
	Currency     string `json:"currency"`
}

const dbTimeout = time.Second * 6

func (e *Exchange) Get(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
		defer cancel()

		exchangeName := r.PathValue("exchangeName")

		query := pgPool.QueryRow(
			ctx,
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
			http.Error(w, err.Error(), http.StatusNoContent)
			return
		}

		jsonExchange, err := json.Marshal(exchange)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonExchange)
	}
}

func (e *Exchange) Insert(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
		defer cancel()

		var exchange Exchange
		err := json.NewDecoder(r.Body).Decode(&exchange)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		result, err := pgPool.Exec(
			ctx,
			`
				INSERT INTO exchanges(exchange_name, currency)
				VALUES($1, $2)
				ON CONFLICT DO NOTHING
			`,
			exchange.ExchangeName, exchange.Currency,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		rowsAffected := result.RowsAffected()
		if rowsAffected == 0 {
			http.Error(w, "Failed to insert exchange", http.StatusConflict)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, rowsAffected)
	}
}
