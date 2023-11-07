package main

import "net/http"

// /systems
func getSystems(w http.ResponseWriter, r *http.Request) {

}

// /systems/metrics/:id
func getSystemMetrics(w http.ResponseWriter, r *http.Request) {

}

// /systems/positions
// /systems/positions/:systemId
func systemPositionsAction(w http.ResponseWriter, r *http.Request) {

}

func getSystemPositions(w http.ResponseWriter, r *http.Request) {

}

func getSystemPositionsForSymbol(w http.ResponseWriter, r *http.Request) {

}

// /systems/market-states/:systemId
func getSystemMarketStates(w http.ResponseWriter, r *http.Request) {

}

// /systems/market-state
func getSystemMarketStateForSymbol(w http.ResponseWriter, r *http.Request) {

}
