package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
	"fmt"
)

type Price struct {
	InstrumentId string `json:"instrument_id"`
	Symbol string `json:"symbol"`
	Open float64 `json:"open"`
	High float64 `json:"high"`
	Low float64 `json:"low"`
	Close float64 `json:"close"`
	Volume int64 `json:"volume"`
	DateTime time.Time `json:"date_time"`
}

type PriceInsertResponse struct {
	Success bool `json:"success"`
	Result string `json:"result"`
	PrevExistingDates []time.Time `json:"prevExistingDates"`
}

func priceDataAction(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
		case http.MethodGet:
			symbol := r.URL.Query().Get("symbol")
			start := r.URL.Query().Get("start")
			end := r.URL.Query().Get("end")

			getPriceData(symbol, start, end, w, r)
		
		case http.MethodPost:
			id := r.URL.Query().Get("id")
			if id == "" {
				http.Error(w, "Invalid request", http.StatusBadRequest)
				return
			}

			var priceData []Price
			if err := json.NewDecoder(r.Body).Decode(&priceData); err != nil {
				http.Error(w, "Invalid request", http.StatusBadRequest)
				return
			}

			insertPriceData(id, priceData, w, r)

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func getPriceData(symbol string, start string, end string, w http.ResponseWriter, r *http.Request) {
	query, err := pgPool.Query(
		context.Background(),
		`
			SELECT instruments.id, instruments.symbol,
				price_data.open_price AS "Open", price_data.high_price AS "High",
				price_data.low_price AS "Low", price_data.close_price AS "Close",
				price_data.volume AS "Volume",
				price_data.date_time AT TIME ZONE 'UTC' AS "Date"
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
			&price.Volume, &price.DateTime,
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

func insertPriceData(id string, priceData []Price, w http.ResponseWriter, r *http.Request) {
	existingDates := []time.Time{}
	priceDataInserts := 0

	for _, price := range priceData {
		// if price.Open == 0 || price.High == 0 ||
		//    price.Low == 0 || price.Close == 0 ||
		//    price.Volume == 0 || price.DateTime == nil {
		// 	incorrectDataPoints = append(incorrectDataPoints, price)
		// } else {
		query := pgPool.QueryRow(
			context.Background(),
			`
				SELECT COUNT(*)
				FROM price_data
				WHERE instrument_id = $1
				AND date_time = $2
			`, 
			id, price.DateTime,
		)

		var dateExists int
		err := query.Scan(&dateExists)
		if err != nil {
			http.Error(w, "", http.StatusInternalServerError)
			return
		}

		if dateExists > 0 {
			existingDates = append(existingDates, price.DateTime)
		} else {
			query := pgPool.QueryRow(
				context.Background(),
				`
					INSERT INTO price_data(
						instrument_id, open_price, high_price,
						low_price, close_price, volume, date_time
					)
					VALUES($1, $2, $3, $4, $5, $6, $7)
				`, 
				id, price.Open, price.High, price.Low, price.Close, 
				price.Volume, price.DateTime,
			)

			var result string
			err := query.Scan(&result)
			if result == "" {
				http.Error(w, "Failed to insert price data", http.StatusInternalServerError)
				return
			}
			if err != nil {
				http.Error(w, "Error while inserting into the database", http.StatusInternalServerError)
				return
			}

			priceDataInserts++
		}
		// }
	}

	priceInsertResponse := PriceInsertResponse {
		Success: true,
		Result: fmt.Sprintf("Inserted %d rows", priceDataInserts),
		PrevExistingDates: existingDates,
	}
	jsonPriceInsertResponse, err := json.Marshal(priceInsertResponse)
	if err != nil {
		http.Error(w, "Error caused by response data", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonPriceInsertResponse)
}

func getFirstAvailableDate(w http.ResponseWriter, r *http.Request) {
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

func getLastAvailableDate(w http.ResponseWriter, r *http.Request) {
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

func getLastDate(w http.ResponseWriter, r *http.Request) {
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
