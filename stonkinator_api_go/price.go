package main

import (
	"encoding/json"
	"net/http"
	"time"
)

type Price struct {
	InstrumentId string `json:"instrument_id"`
	Symbol string `json:"symbol"`
	DateTime time.Time `json:"date_time"`
	Open int `json:"open"`
	High int `json:"high"`
	Low int `json:"low"`
	Close int `json:"close"`
	Volume int64 `json:"volume"`
}

func priceDataAction(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
		case http.MethodGet:
		
		case http.MethodPost:
			var price Price
			err := json.NewDecoder(r.Body).Decode(&price)
			if err != nil {
				http.Error(w, "Invalid request body", http.StatusBadRequest)
				return
			} else {
				insertPriceData(price, w, r)
			}

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// get /price-data/:symbol
func getPriceData(symbol string, w http.ResponseWriter, r *http.Request) {

}

// post /price-data/:id
func insertPriceData(price Price, w http.ResponseWriter, r *http.Request) {

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
