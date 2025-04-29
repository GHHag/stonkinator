package entities

import (
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type TradingSystem struct {
	Id              string    `json:"id,omitempty"`
	Name            string    `json:"name"`
	CurrentDatetime time.Time `json:"current_date_time"`
}

type TradingSystemSubscription struct {
	Id              string `json:"id,omitempty"`
	UserId          string `json:"user_id"`
	TradingSystemId string `json:"trading_system_id"`
}

type Order struct {
	Id              string `json:"id,omitempty"`
	InstrumentId    string `json:"instrument_id"`
	TradingSystemId string `json:"trading_system_id"`
	OrderData       string `json:"order_data,omitempty"`
}

type Position struct {
	Id              string    `json:"id,omitempty"`
	InstrumentId    string    `json:"instrument_id"`
	TradingSystemId string    `json:"trading_system_id"`
	DateTime        time.Time `json:"date_time"`
	PositionData    string    `json:"position_data,omitempty"`
}

type MarketState struct {
	Id              string `json:"id,omitempty"`
	InstrumentId    string `json:"instrument_id"`
	TradingSystemId string `json:"trading_system_id"`
	Metrics         string `json:"metrics"`
}

func (ts *TradingSystem) GetOne(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// query := pgPool.QueryRow(
		// 	context.Background(),
		// 	`
		// 		SELECT
		// 			trading_systems.id, trading_systems.trading_system_name,
		// 				trading_systems.current_date_time
		// 			instruments.symbol
		// 			trading_system_market_states.metrics
		// 		FROM trading_systems, instruments, trading_system_market_states
		// 		WHERE trading_systems.id = trading_system_market_states.trading_system_id
		// 		AND instruments.id = trading_system_market_states.instrument_id
		// 		AND trading_systems.id = $1
		// 	`,
		// )
	}
}

func (ts *TradingSystem) GetMany(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// query := pgPool.Query(
		// 	context.Background(),
		// 	`
		// 		SELECT *
		// 		FROM trading_systems
		// 	`,
		// )
	}
}

func (ts *TradingSystem) SubscribeTo(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

	}
}

func (ts *TradingSystem) GetSubscriptions(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

	}
}

func (ts *TradingSystem) LinkToWatchlist(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

	}
}
func (ms *MarketState) Get(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

	}
}

func (o *Order) Get(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

	}
}

func (p *Position) GetOne(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

	}
}

func (p *Position) GetMany(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

	}
}

func (p *Position) GetLatest(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

	}
}
