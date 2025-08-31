package main

import (
	"context"
	"io"
	pb "stonkinator_rpc_service/stonkinator_rpc_service"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *server) InsertExchange(ctx context.Context, req *pb.Exchange) (*pb.CUD, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	result, err := s.pgPool.Exec(
		ctx,
		`
			INSERT INTO exchanges(exchange_name, currency)
			VALUES($1, $2)
			ON CONFLICT DO NOTHING
		`,
		req.Name, req.Currency,
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res := &pb.CUD{
		NumAffected: int32(result.RowsAffected()),
	}

	return res, nil
}

func (s *server) GetExchange(ctx context.Context, req *pb.GetBy) (*pb.Exchange, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	query := s.pgPool.QueryRow(
		ctx,
		`
			SELECT id, exchange_name
			FROM exchanges
			WHERE UPPER(exchange_name) = $1
		`,
		strings.ToUpper(req.GetStrIdentifier()),
	)

	res := &pb.Exchange{}
	err := query.Scan(&res.Id, &res.Name)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	return res, nil
}

func (s *server) GetExchanges(ctx context.Context, req *pb.GetAll) (*pb.Exchanges, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	query, err := s.pgPool.Query(
		ctx,
		`
			SELECT id, exchange_name
			FROM exchanges
		`,
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}
	defer query.Close()

	exchanges := []*pb.Exchange{}
	for query.Next() {
		var exchange pb.Exchange
		err := query.Scan(
			&exchange.Id,
			&exchange.Name,
		)

		if err == nil {
			exchanges = append(exchanges, &exchange)
		}
	}

	res := &pb.Exchanges{
		Exchanges: exchanges,
	}

	return res, nil
}

func (s *server) InsertInstrument(ctx context.Context, req *pb.Instrument) (*pb.CUD, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	result, err := s.pgPool.Exec(
		ctx,
		`
			INSERT INTO instruments(exchange_id, instrument_name, symbol, sector)
			VALUES($1, $2, $3, $4)
			ON CONFLICT DO NOTHING
		`,
		req.ExchangeId, req.Name, strings.ToUpper(req.Symbol), req.Sector,
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res := &pb.CUD{
		NumAffected: int32(result.RowsAffected()),
	}

	return res, nil
}

func (s *server) GetInstrument(ctx context.Context, req *pb.GetBy) (*pb.Instrument, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	query := s.pgPool.QueryRow(
		ctx,
		`
			SELECT id, exchange_id, instrument_name, symbol, sector
			FROM instruments
			WHERE symbol = $1
		`,
		strings.ToUpper(req.GetStrIdentifier()),
	)

	res := &pb.Instrument{}
	err := query.Scan(&res.Id, &res.ExchangeId, &res.Name, &res.Symbol, &res.Sector)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	return res, nil
}

func (s *server) GetDateTime(ctx context.Context, req *pb.GetDateTimeRequest) (*pb.DateTime, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	var queryStr string
	if req.Min {
		queryStr = `
			SELECT MIN(price_data.date_time)
			FROM instruments, price_data
			WHERE instruments.id = price_data.instrument_id
			AND instruments.id = $1
		`
	} else {
		queryStr = `
			SELECT MAX(price_data.date_time)
			FROM instruments, price_data
			WHERE instruments.id = price_data.instrument_id
			AND instruments.id = $1
		`
	}

	query := s.pgPool.QueryRow(
		ctx,
		queryStr,
		req.InstrumentId,
	)

	var dateTime time.Time
	err := query.Scan(&dateTime)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res := &pb.DateTime{
		DateTime: dateTime.Format(DATE_TIME_FORMAT),
	}

	return res, nil
}

func (s *server) GetLastDate(ctx context.Context, req *pb.GetBy) (*pb.DateTime, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	query := s.pgPool.QueryRow(
		ctx,
		`
			WITH instrument_one_dates AS (
				SELECT date_time
				FROM instruments, price_data
				WHERE instruments.id = price_data.instrument_id
				AND instruments.symbol = $1
				ORDER BY price_data.date_time DESC
				LIMIT 20
			),
			instrument_two_dates AS (
				SELECT date_time
				FROM instruments, price_data
				WHERE instruments.id = price_data.instrument_id
				AND instruments.symbol = $2
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
		strings.ToUpper(req.GetStrIdentifier()), strings.ToUpper(req.GetAltStrIdentifier()),
	)

	var dateTime time.Time
	err := query.Scan(&dateTime)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res := &pb.DateTime{
		DateTime: dateTime.Format(DATE_TIME_FORMAT),
	}

	return res, nil
}

func (s *server) InsertPrice(ctx context.Context, req *pb.Price) (*pb.CUD, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	dateTime := time.Unix(int64(req.Timestamp.UnixTimestampSeconds), 0).UTC().Truncate(time.Minute).Format(DATE_FORMAT)

	result, err := s.pgPool.Exec(
		ctx,
		`
			INSERT INTO price_data(
				instrument_id, open_price, high_price,
				low_price, close_price, volume, date_time
			)
			VALUES($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT DO NOTHING
		`,
		req.InstrumentId, req.Open, req.High, req.Low, req.Close,
		req.Volume, dateTime,
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res := &pb.CUD{
		NumAffected: int32(result.RowsAffected()),
	}

	return res, nil
}

func (s *server) InsertPriceData(stream pb.SecuritiesService_InsertPriceDataServer) error {
	ctx, cancel := context.WithTimeout(stream.Context(), DB_TIMEOUT)
	defer cancel()

	batch := &pgx.Batch{}
	var count int
	for {
		price, err := stream.Recv()
		if err == io.EOF {
			batchQueryResult := s.pgPool.SendBatch(ctx, batch)
			defer batchQueryResult.Close()

			var numAffected int64
			for i := 0; i < count; i++ {
				commandTag, err := batchQueryResult.Exec()
				if err != nil {
					s.errorLog.Printf("%s, instrument id: %s", err, price.InstrumentId)
				} else {
					numAffected += commandTag.RowsAffected()
				}
			}

			return stream.SendAndClose(&pb.CUD{NumAffected: int32(numAffected)})
		}
		if err != nil {
			s.errorLog.Printf("%s, instrument id: %s", err, price.InstrumentId)
			return err
		}

		dateTime := time.Unix(int64(price.Timestamp.UnixTimestampSeconds), 0).UTC().Truncate(time.Minute).Format(DATE_FORMAT)
		batch.Queue(
			`
				INSERT INTO price_data(
					instrument_id, open_price, high_price,
					low_price, close_price, volume, date_time
				)
				VALUES($1, $2, $3, $4, $5, $6, $7)
				ON CONFLICT DO NOTHING
			`,
			price.InstrumentId, price.Open, price.High, price.Low, price.Close,
			price.Volume, dateTime,
		)

		count++
	}
}

func (s *server) GetPriceData(req *pb.GetPriceDataRequest, stream pb.SecuritiesService_GetPriceDataServer) error {
	ctx, cancel := context.WithTimeout(stream.Context(), DB_TIMEOUT)
	defer cancel()

	query, err := s.pgPool.Query(
		ctx,
		`
			SELECT instrument_id,
				open_price AS "open", high_price AS "high",
				low_price AS "low", close_price AS "close",
				volume AS "volume", date_time AS "date"
			FROM price_data
			WHERE instrument_id = $1
			AND date_time >= $2
			AND date_time <= $3
			ORDER BY date_time
		`,
		req.InstrumentId, req.StartDateTime.DateTime, req.EndDateTime.DateTime,
	)
	if err != nil {
		s.errorLog.Println(err)
		return err
	}
	defer query.Close()

	for query.Next() {
		var price pb.Price
		var dateTime time.Time
		err = query.Scan(
			&price.InstrumentId,
			&price.Open,
			&price.High,
			&price.Low,
			&price.Close,
			&price.Volume,
			&dateTime,
		)
		if err != nil {
			s.errorLog.Println(err)
			continue
		}

		price.Timestamp = &pb.Timestamp{
			UnixTimestampSeconds: uint64(dateTime.Truncate(time.Minute).Unix()),
		}
		if err := stream.Send(&price); err != nil {
			s.errorLog.Println(err)
			return err
		}
	}

	return nil
}

func (s *server) GetExchangeInstruments(ctx context.Context, req *pb.GetBy) (*pb.Instruments, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	query, err := s.pgPool.Query(
		ctx,
		`
			SELECT instruments.id, instruments.exchange_id, instruments.instrument_name,
				instruments.symbol, instruments.sector
			FROM exchanges, instruments
			WHERE exchanges.id = instruments.exchange_id
			AND exchanges.id = $1
		`,
		req.GetStrIdentifier(),
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}
	defer query.Close()

	instruments := []*pb.Instrument{}
	for query.Next() {
		var instrument pb.Instrument
		err = query.Scan(
			&instrument.Id,
			&instrument.ExchangeId,
			&instrument.Name,
			&instrument.Symbol,
			&instrument.Sector,
		)

		if err == nil {
			instruments = append(instruments, &instrument)
		}
	}

	res := &pb.Instruments{
		Instruments: instruments,
	}

	return res, nil
}

func (s *server) GetMarketListInstruments(ctx context.Context, req *pb.GetBy) (*pb.Instruments, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	query, err := s.pgPool.Query(
		ctx,
		`
			SELECT instruments.id, instruments.exchange_id, instruments.instrument_name,
				instruments.symbol, instruments.sector
			FROM instruments, market_lists, market_list_instruments
			WHERE instruments.id = market_list_instruments.instrument_id
			AND market_lists.id = market_list_instruments.market_list_id
			AND UPPER(market_lists.market_list) = $1
		`,
		strings.ToUpper(req.GetStrIdentifier()),
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}
	defer query.Close()

	instruments := []*pb.Instrument{}
	for query.Next() {
		var instrument pb.Instrument
		err = query.Scan(
			&instrument.Id,
			&instrument.ExchangeId,
			&instrument.Name,
			&instrument.Symbol,
			&instrument.Sector,
		)

		if err == nil {
			instruments = append(instruments, &instrument)
		}
	}

	res := &pb.Instruments{
		Instruments: instruments,
	}

	return res, nil
}

// func (s *server) GetMarketListInstrumentsPriceData(ctx context.Context, req *pb.MarketList) (*pb.GetPriceDataResponse, error) {
// ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
// defer cancel()

// 	res := &pb.GetPriceDataResponse{}

// 	return res, nil
// }
