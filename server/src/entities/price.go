package entities

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Price struct {
	InstrumentId string    `json:"instrument-id,omitempty"`
	Index        int64     `json:"index,omitempty"`
	Symbol       string    `json:"symbol"`
	Date         time.Time `json:"date"`
	Open         float64   `json:"open"`
	High         float64   `json:"high"`
	Low          float64   `json:"low"`
	Close        float64   `json:"close"`
	Volume       float64   `json:"volume"`
}

func (p *Price) Get(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), DB_TIMEOUT)
		defer cancel()

		symbol := r.PathValue("symbol")
		start := r.URL.Query().Get("start")
		end := r.URL.Query().Get("end")

		query, err := pgPool.Query(
			ctx,
			`
				SELECT instruments.id, instruments.symbol,
					price_data.open_price AS "open", price_data.high_price AS "high",
					price_data.low_price AS "low", price_data.close_price AS "close",
					price_data.volume AS "volume", price_data.date_time "date"
				FROM instruments, price_data
				WHERE instruments.id = price_data.instrument_id
				AND UPPER(instruments.symbol) = $1
				AND price_data.date_time >= $2
				AND price_data.date_time <= $3
				ORDER BY price_data.date_time
			`,
			strings.ToUpper(symbol), start, end,
		)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer query.Close()

		priceData := []Price{}
		for query.Next() {
			var price Price
			err = query.Scan(
				&price.InstrumentId, &price.Symbol,
				&price.Open, &price.High,
				&price.Low, &price.Close,
				&price.Volume, &price.Date,
			)
			if err == nil {
				priceData = append(priceData, price)
			}
		}

		jsonPriceData, err := json.Marshal(priceData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var httpStatus int
		if len(priceData) > 0 {
			httpStatus = http.StatusOK
		} else {
			httpStatus = http.StatusNoContent
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpStatus)
		w.Write(jsonPriceData)
	}
}
