package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"stonkinator/entities"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type app struct {
	pgPool   *pgxpool.Pool
	infoLog  *log.Logger
	errorLog *log.Logger
	entities entities.Entities
	server   *http.Server
}

func (app *app) create(apiUrl string) {
	// Have the servemux as a part of the app struct or not? Decide when implementing auth?
	serveMux := http.NewServeMux()
	app.register(apiUrl, serveMux, app.pgPool)

	// So you want to expose Go on the Internet
	// https://blog.cloudflare.com/exposing-go-on-the-internet/
	// https://blog.gopheracademy.com/advent-2016/exposing-go-on-the-internet/
	tlsConfig := &tls.Config{
		PreferServerCipherSuites: true,
		CurvePreferences: []tls.CurveID{
			tls.CurveP256,
			tls.X25519,
		},
	}

	app.server = &http.Server{
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
		TLSConfig:    tlsConfig,
		Handler:      serveMux,
	}
}

func (app *app) run(port string, certFile string, keyFile string) error {
	tcpListener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return err
	}

	listener := tcpKeepAliveListener{
		TCPListener:     tcpListener.(*net.TCPListener),
		keepAlivePeriod: 1 * time.Minute,
	}

	// What are the differences in the implementations of ServeTLS and ListenAndServeTLS?
	err = app.server.ServeTLS(listener, certFile, keyFile)

	return err
}

func (app *app) register(apiUrl string, serveMux *http.ServeMux, pgPool *pgxpool.Pool) {
	serveMux.HandleFunc(fmt.Sprintf("GET %s/health-check", apiUrl), healthCheck)

	serveMux.HandleFunc(fmt.Sprintf("GET %s/exchange/{exchangeName}", apiUrl), app.entities.Exchange.Get(pgPool))

	serveMux.HandleFunc(fmt.Sprintf("GET %s/instrument/{symbol}", apiUrl), app.entities.Instrument.GetOne(pgPool))
	serveMux.HandleFunc(fmt.Sprintf("GET %s/instruments/{exchangeId}", apiUrl), app.entities.Instrument.GetMany(pgPool))
	serveMux.HandleFunc(fmt.Sprintf("GET %s/instruments/sectors", apiUrl), entities.GetSectors(pgPool))

	serveMux.HandleFunc(fmt.Sprintf("GET %s/market-lists/{exchangeId}", apiUrl), app.entities.MarketList.GetMany(pgPool))

	serveMux.HandleFunc(fmt.Sprintf("POST %s/watchlist", apiUrl), app.entities.Watchlist.Create(pgPool))
	serveMux.HandleFunc(fmt.Sprintf("POST %s/watchlist/instrument", apiUrl), app.entities.Watchlist.AddInstrument(pgPool))
	serveMux.HandleFunc(fmt.Sprintf("DELETE %s/watchlist/instrument/{watchlistInstrumentId}", apiUrl), app.entities.Watchlist.RemoveInstrument(pgPool))
	serveMux.HandleFunc(fmt.Sprintf("GET %s/watchlist/{watchlistId}/instruments", apiUrl), app.entities.Watchlist.GetInstruments(pgPool))
	serveMux.HandleFunc(fmt.Sprintf("PUT %s/watchlist", apiUrl), app.entities.Watchlist.Update(pgPool))
	serveMux.HandleFunc(fmt.Sprintf("DELETE %s/watchlist/{watchlistId}", apiUrl), app.entities.Watchlist.Delete(pgPool))
	serveMux.HandleFunc(fmt.Sprintf("GET %s/watchlists/{userId}", apiUrl), app.entities.Watchlist.GetMany(pgPool))

	serveMux.HandleFunc(fmt.Sprintf("GET %s/price/{symbol}", apiUrl), app.entities.Price.Get(pgPool))

	serveMux.HandleFunc(fmt.Sprintf("GET %s/system/{systemName}", apiUrl), app.entities.TradingSystem.GetOne(pgPool))
	serveMux.HandleFunc(fmt.Sprintf("GET %s/systems", apiUrl), app.entities.TradingSystem.GetMany(pgPool))
	serveMux.HandleFunc(fmt.Sprintf("POST %s/systems/{systemId}/subscribe", apiUrl), app.entities.TradingSystem.SubscribeTo(pgPool))
	serveMux.HandleFunc(fmt.Sprintf("GET %s/systems/{userId}/subscribed", apiUrl), app.entities.TradingSystem.GetSubscriptions(pgPool))
	serveMux.HandleFunc(fmt.Sprintf("POST %s/systems/{subscriptionId}/watchlist/{watchlistId}", apiUrl), app.entities.TradingSystem.LinkToWatchlist(pgPool))
	serveMux.HandleFunc(fmt.Sprintf("GET %s/systems/{systemId}/market-state", apiUrl), app.entities.MarketState.Get(pgPool))
	serveMux.HandleFunc(fmt.Sprintf("GET %s/systems/{systemId}/order", apiUrl), app.entities.Order.Get(pgPool))
	serveMux.HandleFunc(fmt.Sprintf("GET %s/systems/{systemId}/position", apiUrl), app.entities.Position.GetOne(pgPool))
	serveMux.HandleFunc(fmt.Sprintf("GET %s/systems/{systemId}/positions", apiUrl), app.entities.Position.GetMany(pgPool))
	serveMux.HandleFunc(fmt.Sprintf("GET %s/systems/{systemId}/latest-position", apiUrl), app.entities.Position.GetLatest(pgPool))

	serveMux.HandleFunc(fmt.Sprintf("POST %s/user/register", apiUrl), app.entities.User.Register(pgPool))
	serveMux.HandleFunc(fmt.Sprintf("POST %s/user/login", apiUrl), app.entities.User.Login(pgPool))
	serveMux.HandleFunc(fmt.Sprintf("DELETE %s/user/logout", apiUrl), app.entities.User.Logout(pgPool))
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	response := map[string]interface{}{
		"message": "Health OK",
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

type tcpKeepAliveListener struct {
	*net.TCPListener
	keepAlivePeriod time.Duration
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(ln.keepAlivePeriod)
	return tc, nil
}
