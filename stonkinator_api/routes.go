package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/GHHag/gobware"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/mongo"
)

func testGobware() gobware.ACL {
	x := gobware.NewACL("user-role")
	return x
}

func register(port string, api_url string, pgPool *pgxpool.Pool, mdb *mongo.Database) {
	http.HandleFunc(fmt.Sprintf("%s/health-check", api_url), healthCheck)

	http.HandleFunc(fmt.Sprintf("%s/user", api_url), registerUser(pgPool))

	http.HandleFunc(fmt.Sprintf("%s/exchange", api_url), exchangeAction(pgPool))

	http.HandleFunc(fmt.Sprintf("%s/instrument", api_url), instrumentAction(pgPool))

	http.HandleFunc(fmt.Sprintf("%s/price", api_url), priceDataAction(pgPool))
	http.HandleFunc(fmt.Sprintf("%s/price/first-dt", api_url), getFirstAvailableDate(pgPool))
	http.HandleFunc(fmt.Sprintf("%s/price/last-dt", api_url), getLastAvailableDate(pgPool))
	http.HandleFunc(fmt.Sprintf("%s/price/date", api_url), getLastDate(pgPool))

	http.HandleFunc(fmt.Sprintf("%s/market-list", api_url), marketListAction(mdb))
	http.HandleFunc(fmt.Sprintf("%s/market-lists", api_url), getMarketLists(mdb))

	http.HandleFunc(fmt.Sprintf("%s/instruments", api_url), instrumentsAction(mdb))
	http.HandleFunc(fmt.Sprintf("%s/instruments/sector", api_url), getSectorInstruments(mdb))
	http.HandleFunc(fmt.Sprintf("%s/instruments/sectors", api_url), getSectors(mdb))
	http.HandleFunc(fmt.Sprintf("%s/instruments/sector/market-lists", api_url), getSectorInstrumentsForMarketLists(mdb))

	http.HandleFunc(fmt.Sprintf("%s/systems", api_url), getSystems(mdb))
	http.HandleFunc(fmt.Sprintf("%s/systems/metrics", api_url), getSystemMetrics(mdb))
	http.HandleFunc(fmt.Sprintf("%s/systems/positions", api_url), systemPositionsAction(mdb))
	http.HandleFunc(fmt.Sprintf("%s/systems/market-states", api_url), getSystemMarketStates(mdb))
	http.HandleFunc(fmt.Sprintf("%s/systems/market-state", api_url), getSystemMarketStateForSymbol(mdb))

	// http.HandleFunc(fmt.Sprintf("%s/market-breadth", api_url), )
	// http.HandleFunc(fmt.Sprintf("%s/sector-breadth", api_url), )

	fport := fmt.Sprintf(":%s", port)
	http.ListenAndServe(fport, nil)
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	response := map[string]interface{}{
		"message": "Health OK",
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
