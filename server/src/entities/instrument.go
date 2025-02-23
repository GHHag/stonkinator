package entities

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"

	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Instrument struct {
	Id         string         `json:"id,omitempty"`
	ExchangeId string         `json:"exchange-id,omitempty"`
	Name       sql.NullString `json:"name"`
	Symbol     string         `json:"symbol"`
	Sector     sql.NullString `json:"sector,omitempty"`
}

type MarketList struct {
	Id         string `json:"id,omitempty"`
	ExchangeId string `json:"exchange-id"`
	MarketList string `json:"market-list"`
}

type MarketListInstrument struct {
	Id           string `json:"id,omitempty"`
	InstrumentId string `json:"instrument-id"`
	MarketListId string `json:"market-list-id"`
}

type Watchlist struct {
	Id     string `json:"id,omitempty"`
	UserId string `json:"user-id"`
	Name   string `json:"name"`
}

type WatchlistInstrument struct {
	Id           string `json:"id,omitempty"`
	InstrumentId string `json:"instrument-id"`
	WatchlistId  string `json:"watchlist-id"`
}

func (i *Instrument) GetOne(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		symbol := r.PathValue("symbol")

		query := pgPool.QueryRow(
			context.Background(),
			`
				SELECT id, exchange_id, instrument_name, symbol, sector
				FROM instruments
				WHERE UPPER(symbol) = $1
			`,
			strings.ToUpper(symbol),
		)

		var instrument Instrument
		err := query.Scan(&instrument.Id, &instrument.ExchangeId, &instrument.Name, &instrument.Symbol, &instrument.Sector)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		jsonInstrument, err := json.Marshal(instrument)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonInstrument)
	}
}

func (i *Instrument) Insert(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var instrument Instrument
		err := json.NewDecoder(r.Body).Decode(&instrument)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		result, err := pgPool.Exec(
			context.Background(),
			`
				INSERT INTO instruments(exchange_id, instrument_name, symbol, sector)
				VALUES($1, $2, $3, $4)
				ON CONFLICT DO NOTHING
			`,
			instrument.ExchangeId, instrument.Name, instrument.Symbol, instrument.Sector,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		rowsAffected := result.RowsAffected()
		if rowsAffected == 0 {
			http.Error(w, "Failed to insert instrument", http.StatusConflict)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, rowsAffected)
	}
}

func (i *Instrument) Update(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("instrument-id")
		fmt.Println("update instrument with id: ", id)

		var instrument Instrument
		err := json.NewDecoder(r.Body).Decode(&instrument)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// TODO: Implement
	}
}

func queryInstrumentsByMarketListIdAndSector(pgPool *pgxpool.Pool, exchangeId string, marketListId string, sector string) (pgx.Rows, error) {
	query, err := pgPool.Query(
		context.Background(),
		`
			SELECT instruments.id, instruments.exchange_id, instruments.instrument_name,
				instruments.symbol, instruments.sector
			FROM exchanges, instruments, market_lists, market_list_instruments
			WHERE exchanges.id = instruments.exchange_id
			AND exchanges.id = market_lists.exchange_id
			AND instruments.id = market_list_instruments.instrument_id
			AND market_lists.id = market_list_instruments.market_list_id
			AND exchanges.id = $1
			AND market_list_instruments.market_list_id = $2
			AND UPPER(instruments.sector) = $3
		`,
		exchangeId, marketListId, strings.ToUpper(sector),
	)

	return query, err

}

func queryInstrumentsByMarketListId(pgPool *pgxpool.Pool, exchangeId string, marketListId string) (pgx.Rows, error) {
	query, err := pgPool.Query(
		context.Background(),
		`
			SELECT instruments.id, instruments.exchange_id, instruments.instrument_name,
				instruments.symbol, instruments.sector
			FROM exchanges, instruments, market_lists, market_list_instruments
			WHERE exchanges.id = instruments.exchange_id
			AND exchanges.id = market_lists.exchange_id
			AND instruments.id = market_list_instruments.instrument_id
			AND market_lists.id = market_list_instruments.market_list_id
			AND exchanges.id = $1
			AND market_list_instruments.market_list_id = $2
		`,
		exchangeId, marketListId,
	)

	return query, err
}

func queryInstrumentsBySector(pgPool *pgxpool.Pool, exchangeId string, sector string) (pgx.Rows, error) {
	query, err := pgPool.Query(
		context.Background(),
		`
			SELECT instruments.id, instruments.exchange_id, instruments.instrument_name,
				instruments.symbol, instruments.sector
			FROM exchanges, instruments
			WHERE exchanges.id = instruments.exchange_id
			AND exchanges.id = $1
			AND UPPER(instruments.sector) = $2
		`,
		exchangeId, strings.ToUpper(sector),
	)

	return query, err
}

func (i *Instrument) GetMany(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		exchangeId := r.PathValue("exchangeId")
		marketListId := r.URL.Query().Get("market-list-id")
		sector := r.URL.Query().Get("sector")

		var query pgx.Rows
		var err error
		if marketListId != "" && sector != "" {
			query, err = queryInstrumentsByMarketListIdAndSector(pgPool, exchangeId, marketListId, sector)
		} else if marketListId != "" {
			query, err = queryInstrumentsByMarketListId(pgPool, exchangeId, marketListId)
		} else if sector != "" {
			query, err = queryInstrumentsBySector(pgPool, exchangeId, sector)
		} else {
			query, err = pgPool.Query(
				context.Background(),
				`
					SELECT instruments.id, instruments.exchange_id, instruments.instrument_name,
						instruments.symbol, instruments.sector
					FROM exchanges, instruments
					WHERE exchanges.id = instruments.exchange_id
					AND exchanges.id = $1
				`,
				exchangeId,
			)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer query.Close()

		instruments := []Instrument{}
		for query.Next() {
			var instrument Instrument
			err = query.Scan(
				&instrument.Id,
				&instrument.ExchangeId,
				&instrument.Name,
				&instrument.Symbol,
				&instrument.Sector,
			)
			if err == nil {
				instruments = append(instruments, instrument)
			}
		}

		jsonInstruments, err := json.Marshal(instruments)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var httpStatus int
		if len(instruments) > 0 {
			httpStatus = http.StatusOK
		} else {
			httpStatus = http.StatusNoContent
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpStatus)
		w.Write(jsonInstruments)
	}
}

func GetSectors(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query, err := pgPool.Query(
			context.Background(),
			`
				SELECT DISTINCT(sector)
				FROM instruments
				WHERE sector IS NOT NULL
				AND sector != ''
			`,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer query.Close()

		sectors := []string{}
		for query.Next() {
			var sector string
			err = query.Scan(&sector)
			if err == nil {
				sectors = append(sectors, sector)
			}
		}

		jsonSectors, err := json.Marshal(sectors)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonSectors)
	}
}

func (ml *MarketList) Insert(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var marketList MarketList
		err := json.NewDecoder(r.Body).Decode(&marketList)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		query := pgPool.QueryRow(
			context.Background(),
			`
				INSERT INTO market_lists(exchange_id, market_list)
				VALUES($1, $2)
				ON CONFLICT DO NOTHING
				RETURNING id
			`,
			marketList.ExchangeId, marketList.MarketList,
		)
		var marketListId string
		err = query.Scan(&marketListId)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(marketListId))
	}
}

func (ml *MarketList) GetId(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		marketList := r.PathValue("marketList")

		query := pgPool.QueryRow(
			context.Background(),
			`
				SELECT id
				FROM market_lists
				WHERE UPPER(market_list) = $1
			`,
			strings.ToUpper(marketList),
		)

		var marketListId string
		err := query.Scan(&marketListId)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(marketListId))
	}
}

func (ml *MarketList) Update(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// TODO: Implement
	}
}

func (ml *MarketList) GetMany(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		exchangeId := r.PathValue("exchangeId")

		query, err := pgPool.Query(
			context.Background(),
			`
				SELECT id, exchange_id, market_list
				FROM market_lists
				WHERE exchange_id = $1
			`,
			exchangeId,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer query.Close()

		marketLists := []MarketList{}
		for query.Next() {
			var marketList MarketList
			err = query.Scan(
				&marketList.Id,
				&marketList.ExchangeId,
				&marketList.MarketList,
			)
			if err == nil {
				marketLists = append(marketLists, marketList)
			}
		}

		jsonMarketLists, err := json.Marshal(marketLists)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonMarketLists)
	}
}

func (ml *MarketList) AddInstrument(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var marketListInstrument MarketListInstrument
		err := json.NewDecoder(r.Body).Decode(&marketListInstrument)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		result, err := pgPool.Exec(
			context.Background(),
			`
				INSERT INTO market_list_instruments(instrument_id, market_list_id)
				VALUES($1, $2)
				ON CONFLICT(instrument_id, market_list_id) DO NOTHING
			`,
			marketListInstrument.InstrumentId, marketListInstrument.MarketListId,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if result.RowsAffected() == 0 {
			http.Error(w, "No rows inserted", http.StatusConflict)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}

func (ml *MarketList) RemoveInstrument(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		marketListInstrumentId := r.PathValue("marketListInstrumentId")

		result, err := pgPool.Exec(
			context.Background(),
			`
				DELETE
				FROM market_list_instruments
				WHERE id = $1
			`,
			marketListInstrumentId,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if result.RowsAffected() == 0 {
			http.Error(w, "No rows deleted", http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func (w *Watchlist) Create(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var watchlist Watchlist
		err := json.NewDecoder(r.Body).Decode(&watchlist)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		query := pgPool.QueryRow(
			context.Background(),
			`
				INSERT INTO watchlists(user_id, watchlist_name)
				VALUES($1, $2)
				RETURNING id
			`,
			watchlist.UserId, watchlist.Name,
		)
		var watchlistId string
		err = query.Scan(&watchlistId)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(watchlistId))
	}
}

func (w *Watchlist) AddInstrument(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var watchlistInstrument WatchlistInstrument
		err := json.NewDecoder(r.Body).Decode(&watchlistInstrument)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		result, err := pgPool.Exec(
			context.Background(),
			`
				INSERT INTO watchlist_instruments(watchlist_id, instrument_id)
				VALUES($1, $2)
				ON CONFLICT(watchlist_id, instrument_id) DO NOTHING
			`,
			watchlistInstrument.WatchlistId, watchlistInstrument.InstrumentId,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}

		if result.RowsAffected() == 0 {
			http.Error(w, "No rows inserted", http.StatusConflict)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}

func (w Watchlist) RemoveInstrument(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		watchlistInstrumentId := r.PathValue("watchlistInstrumentId")

		result, err := pgPool.Exec(
			context.Background(),
			`
				DELETE
				FROM watchlist_instruments
				WHERE id = $1
			`,
			watchlistInstrumentId,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if result.RowsAffected() == 0 {
			http.Error(w, "No rows deleted", http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func (w *Watchlist) GetInstruments(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		watchlistId := r.PathValue("watchlistId")

		query, err := pgPool.Query(
			context.Background(),
			`
				SELECT instruments.id, instruments.instrument_name, instruments.symbol
				FROM instruments, watchlists, watchlist_instruments
				WHERE instruments.id = watchlist_instruments.instrument_id
				AND watchlists.id = watchlist_instruments.watchlist_id
				AND watchlists.id = $1
			`,
			watchlistId,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		defer query.Close()

		instruments := []Instrument{}
		for query.Next() {
			var instrument Instrument
			err = query.Scan(
				&instrument.Id,
				&instrument.Name,
				&instrument.Symbol,
			)
			if err == nil {
				instruments = append(instruments, instrument)
			}
		}

		jsonInstruments, err := json.Marshal(instruments)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonInstruments)
	}
}

func (w *Watchlist) Update(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var watchlist Watchlist
		err := json.NewDecoder(r.Body).Decode(&watchlist)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		query := pgPool.QueryRow(
			context.Background(),
			`
				UPDATE watchlists
				SET watchlist_name = $1
				WHERE id = $2
				RETURNING (id, user_id, watchlist_name)
			`,
			watchlist.Name, watchlist.Id,
		)
		var updatedWatchlist Watchlist
		err = query.Scan(&updatedWatchlist)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		jsonWatchlist, err := json.Marshal(updatedWatchlist)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonWatchlist)
	}
}

func (w *Watchlist) Delete(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		watchlistId := r.PathValue("watchlistId")

		result, err := pgPool.Exec(
			context.Background(),
			`
				DELETE
				FROM watchlists
				WHERE id = $1
			`,
			watchlistId,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if result.RowsAffected() == 0 {
			http.Error(w, "No rows deleted", http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func (w *Watchlist) GetMany(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userId := r.PathValue("userId")

		query, err := pgPool.Query(
			context.Background(),
			`
				SELECT watchlists.id, watchlists.watchlist_name
				FROM watchlists
				WHERE user_id = $1
			`,
			userId,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		defer query.Close()

		watchlists := []Watchlist{}
		for query.Next() {
			var watchlist Watchlist
			err = query.Scan(
				&watchlist.Id,
				&watchlist.Name,
			)
			if err == nil {
				watchlists = append(watchlists, watchlist)
			}
		}

		jsonWatchlists, err := json.Marshal(watchlists)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var httpStatus int
		if len(watchlists) > 0 {
			httpStatus = http.StatusOK
		} else {
			httpStatus = http.StatusNoContent
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpStatus)
		w.Write(jsonWatchlists)
	}
}
