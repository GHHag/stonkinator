package entities

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Exchange struct {
	Id           string `json:"id,omitempty"`
	ExchangeName string `json:"exchange-name"`
	Currency     string `json:"currency"`
}

func (e *Exchange) Get(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), DB_TIMEOUT)
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
