package main

import(
	"net/http"
	"encoding/json"
	"fmt"
	"github.com/GHHag/gobware"
)

func testGobware() gobware.ACL {
	x := gobware.NewACL("user-role")
	return x
}

func register(port string, api_url string) {
	http.HandleFunc(fmt.Sprintf("%s/health-check", api_url), healthCheck)

	http.HandleFunc(fmt.Sprintf("%s/exchange/", api_url), exchangeAction)
	http.HandleFunc(fmt.Sprintf("%s/instrument/", api_url), instrumentAction)
	// http.HandleFunc(fmt.Sprintf("%s/price-data", api_url), priceDataAction)
	//
	// http.HandleFunc(fmt.Sprintf("%s/first-dt", api_url), firstDateAction)
	// http.HandleFunc(fmt.Sprintf("%s/last-dt", api_url), lastDateAction)
	// http.HandleFunc(fmt.Sprintf("%s/date", api_url), dateAction)

	// http.HandleFunc(fmt.Sprintf("%s/market-list", api_url), )
	// http.HandleFunc(fmt.Sprintf("%s/market-lists", api_url), )
	//
	// http.HandleFunc(fmt.Sprintf("%s/instruments", api_url), )
	// http.HandleFunc(fmt.Sprintf("%s/instruments/sector", api_url), )
	// http.HandleFunc(fmt.Sprintf("%s/instruments/sectors", api_url), )
	// http.HandleFunc(fmt.Sprintf("%s/instruments/symbols", api_url), )
	// http.HandleFunc(fmt.Sprintf("%s/instruments/sector/market-lists", api_url), )
	//
	// http.HandleFunc(fmt.Sprintf("%s/systems", api_url), )
	// http.HandleFunc(fmt.Sprintf("%s/systems/metrics", api_url), )
	// http.HandleFunc(fmt.Sprintf("%s/systems/positions", api_url), )
	// http.HandleFunc(fmt.Sprintf("%s/systems/market-states", api_url), )
	// http.HandleFunc(fmt.Sprintf("%s/systems/market-state", api_url), )
	//
	// http.HandleFunc(fmt.Sprintf("%s/market-breadth", api_url), )
	// http.HandleFunc(fmt.Sprintf("%s/sector-breadth", api_url), )

	fport := fmt.Sprintf(":%s", port)
	http.ListenAndServe(fport, nil)
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	response := map[string]interface{} {
		"message": "Health OK",
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
