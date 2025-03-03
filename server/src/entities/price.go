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

type PriceInsertResponse struct {
	Result            string   `json:"result"`
	PrevExistingDates []string `json:"prevExistingDates"`
}

type PriceList []Price

func (pl *PriceList) UnmarshalJSON(data []byte) error {
	var auxSlice []struct {
		InstrumentId string  `json:"instrument-id,omitempty"`
		Index        int64   `json:"index,omitempty"`
		Symbol       string  `json:"symbol"`
		DateTime     string  `json:"date"`
		Open         float64 `json:"open"`
		High         float64 `json:"high"`
		Low          float64 `json:"low"`
		Close        float64 `json:"close"`
		Volume       float64 `json:"volume"`
	}

	if err := json.Unmarshal(data, &auxSlice); err != nil {
		return err
	}

	for _, aux := range auxSlice {
		strippedDate := aux.DateTime[:10]
		date, err := time.Parse(time.RFC3339, strippedDate)
		if err != nil {
			return err
		}

		*pl = append(*pl, Price{
			InstrumentId: aux.InstrumentId,
			Index:        aux.Index,
			Symbol:       aux.Symbol,
			Date:         date,
			Open:         aux.Open,
			High:         aux.High,
			Low:          aux.Low,
			Close:        aux.Close,
			Volume:       aux.Volume,
		})
	}

	return nil
}

func (p *Price) Get(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		symbol := r.PathValue("symbol")
		start := r.URL.Query().Get("start")
		end := r.URL.Query().Get("end")

		query, err := pgPool.Query(
			context.Background(),
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
