package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

type Price struct {
	InstrumentId string `json:"instrument_id"`
	Symbol string `json:"symbol"`
	DateTime time.Time `json:"date_time"`
	Open float64 `json:"open"`
	High float64 `json:"high"`
	Low float64 `json:"low"`
	Close float64 `json:"close"`
	Volume int64 `json:"volume"`
}

type PriceInsertResponse struct {
	Success bool `json:"success"`
	Result string `json:"result"`
	PrevExistingDates []time.Time `json:"prevExistingDates"`
	IncorrectData []Price `json:"incorrectData"`
}

// get /price-data/:symbol
// post /price-data/:id
func priceDataAction(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
		case http.MethodGet:
			symbol := r.URL.Query().Get("symbol")
			start := r.URL.Query().Get("start")
			end := r.URL.Query().Get("end")

			// Create time.Time objects of start and end
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

			// if err != nil {
			// 	http.Error(w, "Invalid request body", http.StatusBadRequest)
			// 	return
			// } else {
			// 	insertPriceData(price, w, r)
			// }
			insertPriceData(id, priceData, w, r)

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func getPriceData(symbol string, start time.Time, end time.Time, w http.ResponseWriter, r *http.Request) {
	query := pgPool.Query(
		`
			SELECT instruments.symbol,
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
		`, strings.ToUpper(symbol), start, end,
	)
	
}

func insertPriceData(id string, priceData []Price, w http.ResponseWriter, r *http.Request) {
	// pgPool, err := GetPgPool()
	// if err != nil {
	// 	http.Error(w, "Failed to connect to database", http.StatusInternalServerError)
	// 	return
	// }

	existingDates := []time.Time{}
	priceDataInserts := 0
	// incorrectDataPoints := []Price{}

	for _, price := range priceData {
		// if price.Open == 0 || price.High == 0 ||
		//    price.Low == 0 || price.Close == 0 ||
		//    price.Volume == 0 || price.DateTime == nil {
		// 	incorrectDataPoints = append(incorrectDataPoints, price)
		// } else {
		var dateExists int
		query := pgPool.QueryRow(
			context.Background(),
			`
				SELECT COUNT(*)
				FROM price_data
				WHERE instrument_id = $1
				AND date_time = $2
			`, id, price.DateTime,
		)
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

	var response PriceInsertResponse 
}

// get /first-dt/:symbol
func getFirstAvailableDate() {

}

// get /last-dt/:symbol
func getLastAvailableDate() {

}

// get /date
func getLastDate() {

}
