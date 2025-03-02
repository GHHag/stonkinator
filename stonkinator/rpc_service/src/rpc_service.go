package main

import (
	"context"
	"fmt"
	"log"
	"net"
	pb "stonkinator_rpc_service/stonkinator_rpc_service"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

// const dbTimeout = time.Second * 6
const DATE_FORMAT = "2006-01-02"
const DATETIME_FORMAT = "2006-01-02 15:04:05"

type server struct {
	pgPool *pgxpool.Pool
	pb.UnimplementedStonkinatorServiceServer
}

type service struct {
	infoLog    *log.Logger
	errorLog   *log.Logger
	grpcServer *grpc.Server
	server     *server
}

func (service *service) create(pgPool *pgxpool.Pool) {
	service.grpcServer = grpc.NewServer()
	service.server = &server{
		pgPool: pgPool,
	}
	pb.RegisterStonkinatorServiceServer(service.grpcServer, service.server)
}

func (service *service) run(port string) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return err
	}

	if err := service.grpcServer.Serve(listener); err != nil {
		return err
	}

	return nil
}

func (s *server) InsertExchange(ctx context.Context, req *pb.InsertExchangeRequest) (*pb.InsertResponse, error) {
	// TODO: Use context WithTimeout?
	// ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	// defer cancel()

	result, err := s.pgPool.Exec(
		ctx,
		`
			INSERT INTO exchanges(exchange_name, currency)
			VALUES($1, $2)
			ON CONFLICT DO NOTHING
		`,
		req.ExchangeName, req.Currency,
	)
	if err != nil {
		// TODO: Log errors, define centralized logging m
		return nil, err
	}

	res := &pb.InsertResponse{
		NumAffected: int32(result.RowsAffected()),
	}

	return res, nil
}

func (s *server) GetExchange(ctx context.Context, req *pb.GetByNameRequest) (*pb.GetExchangeResponse, error) {
	query := s.pgPool.QueryRow(
		ctx,
		`
			SELECT id, exchange_name
			FROM exchanges
			WHERE UPPER(exchange_name) = $1
		`,
		strings.ToUpper(req.Name),
	)

	res := &pb.GetExchangeResponse{}
	err := query.Scan(&res.Id, &res.ExchangeName)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (s *server) GetExchanges(ctx context.Context, req *pb.GetAllRequest) (*pb.GetExchangesResponse, error) {
	query, err := s.pgPool.Query(
		ctx,
		`
			SELECT id, exchange_name
			FROM exchanges
		`,
	)
	if err != nil {
		return nil, err
	}
	defer query.Close()

	exchanges := []*pb.GetExchangeResponse{}
	for query.Next() {
		var exchange pb.GetExchangeResponse
		err := query.Scan(
			&exchange.Id,
			&exchange.ExchangeName,
		)

		if err == nil {
			exchanges = append(exchanges, &exchange)
		}
	}

	res := &pb.GetExchangesResponse{
		Exchanges: exchanges,
	}

	return res, nil
}

func (s *server) InsertInstrument(ctx context.Context, req *pb.Instrument) (*pb.InsertResponse, error) {
	result, err := s.pgPool.Exec(
		ctx,
		`
			INSERT INTO instruments(exchange_id, instrument_name, symbol, sector)
			VALUES($1, $2, $3, $4)
			ON CONFLICT DO NOTHING
		`,
		req.ExchangeId, req.InstrumentName, req.Symbol, req.Sector,
	)
	if err != nil {
		return nil, err
	}

	res := &pb.InsertResponse{
		NumAffected: int32(result.RowsAffected()),
	}

	return res, nil
}

func (s *server) GetInstrument(ctx context.Context, req *pb.GetBySymbolRequest) (*pb.Instrument, error) {
	query := s.pgPool.QueryRow(
		ctx,
		`
			SELECT id, exchange_id, instrument_name, symbol, sector
			FROM instruments
			WHERE UPPER(symbol) = $1
		`,
		strings.ToUpper(req.Symbol),
	)

	res := &pb.Instrument{}
	err := query.Scan(&res.Id, &res.ExchangeId, &res.InstrumentName, &res.Symbol, &res.Sector)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (s *server) GetDateTime(ctx context.Context, req *pb.GetDateTimeRequest) (*pb.DateTime, error) {
	var queryStr string
	if req.Min {
		queryStr = `
			SELECT MIN(price_data.date_time)
			FROM instruments, price_data
			WHERE instruments.id = price_data.instrument_id
			AND UPPER(instruments.symbol) = $1
		`
	} else {
		queryStr = `
			SELECT MAX(price_data.date_time)
			FROM instruments, price_data
			WHERE instruments.id = price_data.instrument_id
			AND UPPER(instruments.symbol) = $1
		`
	}

	query := s.pgPool.QueryRow(
		ctx,
		queryStr,
		strings.ToUpper(req.Symbol),
	)

	var dateTime time.Time
	err := query.Scan(&dateTime)
	if err != nil {
		return nil, err
	}

	res := &pb.DateTime{
		DateTime: dateTime.Format(DATETIME_FORMAT),
	}

	return res, nil
}

func (s *server) GetLastDate(ctx context.Context, req *pb.GetLastDateRequest) (*pb.DateTime, error) {
	query := s.pgPool.QueryRow(
		ctx,
		`
			WITH instrument_one_dates AS (
				SELECT date_time
				FROM instruments, price_data
				WHERE instruments.id = price_data.instrument_id
				AND UPPER(instruments.symbol) = $1
				ORDER BY price_data.date_time DESC
				LIMIT 20
			),
			instrument_two_dates AS (
				SELECT date_time
				FROM instruments, price_data
				WHERE instruments.id = price_data.instrument_id
				AND UPPER(instruments.symbol) = $2
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
		strings.ToUpper(req.Symbol_1), strings.ToUpper(req.Symbol_2),
	)

	var dateTime time.Time
	err := query.Scan(&dateTime)
	if err != nil {
		return nil, err
	}

	res := &pb.DateTime{
		DateTime: dateTime.Format(DATETIME_FORMAT),
	}

	return res, nil
}

func (s *server) InsertPriceData(ctx context.Context, req *pb.PriceData) (*pb.InsertResponse, error) {
	dateTime, err := time.Parse(DATE_FORMAT, req.DateTime)
	if err != nil {
		return nil, err
	}

	result, err := s.pgPool.Exec(
		ctx,
		`
			INSERT INTO price_data(
				instrument_id, open_price, high_price,
				low_price, close_price, volume, date_time
			)
			VALUES($1, $2, $3, $4, $5, $6, $7)
		`,
		req.InstrumentId, req.OpenPrice, req.HighPrice, req.LowPrice, req.ClosePrice,
		req.Volume, dateTime,
	)
	if err != nil {
		return nil, err
	}

	res := &pb.InsertResponse{
		NumAffected: int32(result.RowsAffected()),
	}

	return res, nil
}

func (s *server) GetPriceData(ctx context.Context, req *pb.GetPriceDataRequest) (*pb.GetPriceDataResponse, error) {
	startDateTime, err := time.Parse(DATETIME_FORMAT, req.StartDateTime)
	if err != nil {
		return nil, err
	}
	endDateTime, err := time.Parse(DATETIME_FORMAT, req.EndDateTime)
	if err != nil {
		return nil, err
	}

	query, err := s.pgPool.Query(
		ctx,
		`
			SELECT instruments.id,
				price_data.open_price AS "open", price_data.high_price AS "high",
				price_data.low_price AS "low", price_data.close_price AS "close",
				price_data.volume AS "volume", price_data.date_time AS "date"
			FROM instruments, price_data
			WHERE instruments.id = price_data.instrument_id
			AND instruments.id = $1
			AND price_data.date_time >= $2
			AND price_data.date_time <= $3
			ORDER BY price_data.date_time
		`,
		req.InstrumentId, startDateTime, endDateTime,
	)
	if err != nil {
		return nil, err
	}
	defer query.Close()

	priceData := []*pb.PriceData{}
	for query.Next() {
		var price pb.PriceData
		var dateTime time.Time
		err = query.Scan(
			&price.InstrumentId,
			&price.OpenPrice,
			&price.HighPrice,
			&price.LowPrice,
			&price.ClosePrice,
			&price.Volume,
			&dateTime,
		)

		if err == nil {
			price.DateTime = dateTime.Format(DATETIME_FORMAT)
			priceData = append(priceData, &price)
		}
	}

	res := &pb.GetPriceDataResponse{
		PriceData: priceData,
	}

	return res, nil
}

func (s *server) GetExchangeInstruments(ctx context.Context, req *pb.GetByIdRequest) (*pb.Instruments, error) {
	query, err := s.pgPool.Query(
		ctx,
		`
			SELECT instruments.id, instruments.exchange_id, instruments.instrument_name,
				instruments.symbol, instruments.sector
			FROM exchanges, instruments
			WHERE exchanges.id = instruments.exchange_id
			AND exchanges.id = $1
		`,
		req.Id,
	)
	if err != nil {
		return nil, err
	}
	defer query.Close()

	instruments := []*pb.Instrument{}
	for query.Next() {
		var instrument pb.Instrument
		err = query.Scan(
			&instrument.Id,
			&instrument.ExchangeId,
			&instrument.InstrumentName,
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

func (s *server) GetMarketListInstruments(ctx context.Context, req *pb.GetByNameRequest) (*pb.Instruments, error) {
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
		strings.ToUpper(req.Name),
	)
	if err != nil {
		return nil, err
	}
	defer query.Close()

	instruments := []*pb.Instrument{}
	for query.Next() {
		var instrument pb.Instrument
		err = query.Scan(
			&instrument.Id,
			&instrument.ExchangeId,
			&instrument.InstrumentName,
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
// 	res := &pb.GetPriceDataResponse{}

// 	return res, nil
// }

// func (s *server) InsertTradingSystem(ctx context.Context, req *pb.InsertTradingSystemRequest) (*pb.InsertTradingSystemResponse, error) {
// 	res := &pb.InsertTradingSystemResponse{
// 		Successful: true,
// 	}

// 	return res, nil
// }

// func (s *server) InsertTradingSystemMetrics(ctx context.Context, req *pb.InsertTradingSystemMetricsRequest) (*pb.InsertTradingSystemMetricsResponse, error) {
// 	res := &pb.InsertTradingSystemMetricsResponse{
// 		Successful: true,
// 	}

// 	return res, nil
// }
