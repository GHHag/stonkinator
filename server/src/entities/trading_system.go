package entities

import (
	"context"
	"encoding/json"
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
	OrderDataJson   string `json:"order_data_json,omitempty"`
	OrderDataBinary []byte `json:"order_data_binary,omitempty"`
}

type Position struct {
	Id                 string    `json:"id,omitempty"`
	InstrumentId       string    `json:"instrument_id"`
	TradingSystemId    string    `json:"trading_system_id"`
	DateTime           time.Time `json:"date_time"`
	PositionDataJson   string    `json:"position_data_json,omitempty"`
	PositionDataBinary []byte    `json:"position_data_binary,omitempty"`
}

type MarketState struct {
	Id              string `json:"id,omitempty"`
	InstrumentId    string `json:"instrument_id"`
	TradingSystemId string `json:"trading_system_id"`
	Metrics         string `json:"metrics"`
}

type TradingSystemModel struct {
	Id              string `json:"id,omitempty"`
	TradingSystemId string `json:"trading_system_id"`
	InstrumentId    string `json:"instrument_id"`
	SerializedModel []byte `json:"serialized_model"`
}

func (ts *TradingSystem) Insert(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var tradingSystem TradingSystem
		err := json.NewDecoder(r.Body).Decode(&tradingSystem)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		result, err := pgPool.Exec(
			context.Background(),
			`
				INSERT INTO trading_systems(trading_system_name)
				VALUES($1)
			`,
			tradingSystem.Name,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		rowsAffected := result.RowsAffected()
		if rowsAffected == 0 {
			http.Error(w, "Failed to insert trading system", http.StatusConflict)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
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

// func (ts *TradingSystem) InsertMetrics(pgPool *pgxpool.Pool) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {

// 		result, err := pgPool.Exec(
// 			context.Background(),
// 			`
// 				INSERT INTO trading_system_market_states(instrument_id, trading_system_id, metrics)
// 				VALUES($1, $2, $3)
// 			`,
// 		)
// 	}
// }

// func (ts *TradingSystem) UpdateMetrics(pgPool *pgxpool.Pool) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {

// 	}
// }

// func (ts *TradingSystem) GetMetrics(pgPool *pgxpool.Pool) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {

// 	}
// }

func (ts *TradingSystem) UpdateCurrentDateTime(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// result, err := pgPool.Exec(
		// 	`
		// 		UPDATE trading_systems
		// 		SET current_date_time = $1
		// 		WHERE id = $2
		// 	`,
		// )
	}
}

func (ts *TradingSystem) GetCurrentDateTime(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// query := pgPool.QueryRow(
		// 	context.Background(),
		// 	// TODO: Find current_date_time by trading system name or by id?
		// 	`
		// 		SELECT current_date_time
		// 		FROM trading_systems
		// 		WHERE trading_sytems.id = $1
		// 	`,
		// )
	}
}

func (ms *MarketState) Insert(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// result, err := pgPool.Exec(
		// 	context.Background(),
		// 	`
		// 		INSERT INTO trading_system_market_states(instrument_id, trading_system_id, metrics)
		// 		VALUES($1, $2, $3)
		// 	`,
		// )
	}
}

func (ms *MarketState) Update(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// result, err := pgPool.Exec(
		// 	context.Background(),
		// 	`
		// 		UPDATE trading_system_market_states
		// 		SET metrics = $1
		// 		WHERE instrument_id = $2
		// 		AND trading_system_id = $3
		// 	`,
		// )
	}
}

func (ms *MarketState) Get(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

	}
}

// func(ms *MarketState) GetMarketStateForInstrument(pgPool *pgxpool.Pool) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {

// 	}
// }

func (o *Order) Insert(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

	}
}

func (o *Order) Get(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

	}
}

func (p *Position) InsertOne(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

	}
}

func (p *Position) InsertMany(pgPool *pgxpool.Pool) http.HandlerFunc {
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

// func(p *Position) InsertSingleInstrumentPositionList(pgPool *pgxpool.Pool) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {

// 	}
// }

// func(p *Position) InsertSingleInstrumentPosition(pgPool *pgxpool.Pool) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {

// 	}
// }

// func(p *Position) GetSingleInstrumentPositionList(pgPool *pgxpool.Pool) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {

// 	}
// }

// func(p *Position) GetSingleInstrumentLatestPosition(pgPool *pgxpool.Pool) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {

// 	}
// }

func (p *Position) GetLatest(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

	}
}

// func(p *Position) InsertCurrentPosition(pgPool *pgxpool.Pool) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {

// 	}
// }

// func(p *Position) GetCurrentPosition(pgPool *pgxpool.Pool) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {

// 	}
// }

// func(ts *TradingSystem) IncrementNumOfPeriods(pgPool *pgxpool.Pool) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {

// 	}
// }

func (ts *TradingSystem) InsertMLModel(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

	}
}

func (ts *TradingSystem) GetMLModel(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

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
