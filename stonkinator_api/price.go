package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Price struct {
	InstrumentId string    `json:"instrument_id,omitempty"`
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
	Success           bool     `json:"success"`
	Result            string   `json:"result"`
	PrevExistingDates []string `json:"prevExistingDates"`
}

type PriceList []Price

func (pl *PriceList) UnmarshalJSON(data []byte) error {
	var auxSlice []struct {
		InstrumentId string  `json:"instrument_id,omitempty"`
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

func priceDataAction(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getPriceData(w, r, pgPool)

		case http.MethodPost:
			insertPriceData(w, r, pgPool)

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func getPriceData(w http.ResponseWriter, r *http.Request, pgPool *pgxpool.Pool) {
	symbol := r.URL.Query().Get("symbol")
	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")

	query, err := pgPool.Query(
		context.Background(),
		`
			SELECT instruments.id, instruments.symbol,
				price_data.open_price AS "open", price_data.high_price AS "high",
				price_data.low_price AS "low", price_data.close_price AS "close",
				price_data.volume AS "volume",
				price_data.date_time AT TIME ZONE 'UTC' AS "date"
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
		http.Error(w, "Error while executing query", http.StatusInternalServerError)
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
		http.Error(w, "Failed to marshal price data", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonPriceData)
}

func insertPriceData(w http.ResponseWriter, r *http.Request, pgPool *pgxpool.Pool) {
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	var priceData PriceList
	if err := json.NewDecoder(r.Body).Decode(&priceData); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	var existingDates []string
	var priceDataInserts int64
	priceDataInserts = 0

	for _, price := range priceData {
		query := pgPool.QueryRow(
			context.Background(),
			`
				SELECT COUNT(*)
				FROM price_data
				WHERE instrument_id = $1
				AND date_time = $2
			`,
			id, price.Date,
		)

		var dateExists int
		err := query.Scan(&dateExists)
		if err != nil {
			http.Error(w, "", http.StatusInternalServerError)
			return
		}

		if dateExists > 0 {
			existingDates = append(existingDates, price.Date.String())
		} else {
			result, err := pgPool.Exec(
				context.Background(),
				`
					INSERT INTO price_data(
						instrument_id, open_price, high_price,
						low_price, close_price, volume, date_time
					)
					VALUES($1, $2, $3, $4, $5, $6, $7)
				`,
				id, price.Open, price.High, price.Low, price.Close,
				price.Volume, price.Date,
			)
			if err != nil {
				http.Error(w, "Error while inserting into the database", http.StatusInternalServerError)
				return
			}
			rowsAffected := result.RowsAffected()
			if rowsAffected == 0 {
				http.Error(w, "Failed to insert price data", http.StatusInternalServerError)
				return
			}

			priceDataInserts = priceDataInserts + rowsAffected
		}
	}

	priceInsertResponse := PriceInsertResponse{
		Success:           true,
		Result:            fmt.Sprintf("Inserted %d rows", priceDataInserts),
		PrevExistingDates: existingDates,
	}
	jsonPriceInsertResponse, err := json.Marshal(priceInsertResponse)
	if err != nil {
		http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonPriceInsertResponse)
}

func getFirstAvailableDate(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method now allowed", http.StatusMethodNotAllowed)
			return
		}

		symbol := r.URL.Query().Get("symbol")

		query := pgPool.QueryRow(
			context.Background(),
			`
				SELECT MIN(price_data.date_time)
				FROM instruments, price_data
				WHERE instruments.id = price_data.instrument_id
				AND instruments.symbol = $1
			`,
			strings.ToUpper(symbol),
		)

		var dateTime time.Time
		err := query.Scan(&dateTime)
		if err != nil {
			http.Error(w, "Failed to get date", http.StatusInternalServerError)
			return
		}

		jsonDateTime, err := json.Marshal(dateTime)
		if err != nil {
			http.Error(w, "Failed to marshal date", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonDateTime)
	}
}

func getLastAvailableDate(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method now allowed", http.StatusMethodNotAllowed)
			return
		}

		symbol := r.URL.Query().Get("symbol")

		query := pgPool.QueryRow(
			context.Background(),
			`
				SELECT MAX(price_data.date_time)
				FROM instruments, price_data
				WHERE instruments.id = price_data.instrument_id
				AND instruments.symbol = $1
			`,
			strings.ToUpper(symbol),
		)

		var dateTime time.Time
		err := query.Scan(&dateTime)
		if err != nil {
			http.Error(w, "Failed to get date", http.StatusInternalServerError)
			return
		}

		jsonDateTime, err := json.Marshal(dateTime)
		if err != nil {
			http.Error(w, "Failed to marshal date", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonDateTime)
	}
}

func getLastDate(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method now allowed", http.StatusMethodNotAllowed)
			return
		}

		symbol1 := r.URL.Query().Get("symbol1")
		symbol2 := r.URL.Query().Get("symbol2")

		query := pgPool.QueryRow(
			context.Background(),
			`
				WITH instrument_one_dates AS (
					SELECT date_time
					FROM instruments, price_data
					WHERE instruments.id = price_data.instrument_id
					AND UPPER(instruments.symbol) = $1
					ORDER BY price_data.date_time DESC
					LIMIT 20
				),
				instrument_two_dates AS (
					SELECT date_time
					FROM instruments, price_data
					WHERE instruments.id = price_data.instrument_id
					AND UPPER(instruments.symbol) = $2
					ORDER BY price_data.date_time DESC
					LIMIT 20
				)
				SELECT *
				FROM instrument_one_dates
				UNION
				SELECT *
				FROM instrument_two_dates
				ORDER BY date_time
				LIMIT 1
			`,
			strings.ToUpper(symbol1), strings.ToUpper(symbol2),
		)

		var dateTime time.Time
		err := query.Scan(&dateTime)
		if err != nil {
			http.Error(w, "Failed to get date", http.StatusInternalServerError)
			return
		}

		jsonDateTime, err := json.Marshal(dateTime)
		if err != nil {
			http.Error(w, "Failed to marshal date", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonDateTime)
	}
}
