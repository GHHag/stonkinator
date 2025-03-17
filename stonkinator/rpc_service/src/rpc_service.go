package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	pb "stonkinator_rpc_service/stonkinator_rpc_service"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

const DB_TIMEOUT = time.Second * 20
const DATE_FORMAT = "2006-01-02"
const DATETIME_FORMAT = "2006-01-02 15:04:05"
const MAX_MESSAGE_SIZE = 4 * 1024 * 1024

type server struct {
	infoLog  *log.Logger
	errorLog *log.Logger
	pgPool   *pgxpool.Pool
	pb.UnimplementedSecuritiesServiceServer
}

type service struct {
	infoLog    *log.Logger
	errorLog   *log.Logger
	grpcServer *grpc.Server
	server     *server
}

func (service *service) create(pgPool *pgxpool.Pool, certFile string, keyFile string, caFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		service.errorLog.Println(err)
		return err
	}

	ca := x509.NewCertPool()
	caBytes, err := os.ReadFile(caFile)
	if err != nil {
		service.errorLog.Println(err)
		return err
	}
	if ok := ca.AppendCertsFromPEM(caBytes); !ok {
		service.errorLog.Printf("failed to parse %q", caFile)
		return fmt.Errorf("failed to parse %q", caFile)
	}

	tlsConfig := &tls.Config{
		ClientAuth:   tls.RequireAndVerifyClientCert,
		Certificates: []tls.Certificate{cert},
		ClientCAs:    ca,
	}

	keepAliveParams := keepalive.ServerParameters{
		MaxConnectionIdle: 10 * time.Minute,
		Time:              3 * time.Minute,
		Timeout:           20 * time.Second,
	}

	// If all services are inside the same network, you can use local credentials for lightweight security.
	// creds := grpc.LocalCredentials(grpc.LocalConnectionType.UDS)

	service.grpcServer = grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsConfig)),
		grpc.KeepaliveParams(keepAliveParams),
		grpc.MaxRecvMsgSize(MAX_MESSAGE_SIZE),
		grpc.MaxSendMsgSize(MAX_MESSAGE_SIZE),
	)
	service.server = &server{
		infoLog:  service.infoLog,
		errorLog: service.errorLog,
		pgPool:   pgPool,
	}
	pb.RegisterSecuritiesServiceServer(service.grpcServer, service.server)

	return nil
}

func (service *service) run(port string) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		service.errorLog.Println(err)
		return err
	}

	go func() {
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
		<-signalChan
		service.grpcServer.GracefulStop()
	}()

	if err := service.grpcServer.Serve(listener); err != nil {
		service.errorLog.Println(err)
		return err
	}

	return nil
}

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

func (s *server) GetExchanges(ctx context.Context, req *pb.GetAllRequest) (*pb.Exchanges, error) {
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
		req.ExchangeId, req.Name, req.Symbol, req.Sector,
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
			WHERE UPPER(symbol) = $1
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
		DateTime: dateTime.Format(DATETIME_FORMAT),
	}

	return res, nil
}

func (s *server) GetLastDate(ctx context.Context, req *pb.GetLastDateRequest) (*pb.DateTime, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

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
		s.errorLog.Println(err)
		return nil, err
	}

	res := &pb.DateTime{
		DateTime: dateTime.Format(DATETIME_FORMAT),
	}

	return res, nil
}

func (s *server) InsertPrice(ctx context.Context, req *pb.Price) (*pb.CUD, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	dateTime, err := time.Parse(DATE_FORMAT, req.DateTime.DateTime)
	if err != nil {
		s.errorLog.Println(err)
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
			ON CONFLICT DO NOTHING
		`,
		req.InstrumentId, req.OpenPrice, req.HighPrice, req.LowPrice, req.ClosePrice,
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

func (s *server) InsertPriceData(ctx context.Context, req *pb.PriceData) (*pb.CUD, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	batch := &pgx.Batch{}

	var err error
	for _, price := range req.PriceData {
		dateTime, err := time.Parse(DATE_FORMAT, price.DateTime.DateTime)
		if err != nil {
			s.errorLog.Println(err)
		}

		batch.Queue(
			`
				INSERT INTO price_data(
					instrument_id, open_price, high_price,
					low_price, close_price, volume, date_time
				)
				VALUES($1, $2, $3, $4, $5, $6, $7)
				ON CONFLICT DO NOTHING
			`,
			price.InstrumentId, price.OpenPrice, price.HighPrice, price.LowPrice, price.ClosePrice,
			price.Volume, dateTime,
		)
	}

	batchQueryResult := s.pgPool.SendBatch(ctx, batch)
	defer batchQueryResult.Close()

	numAffected := 0

	for range req.PriceData {
		commantTag, err := batchQueryResult.Exec()
		if err != nil {
			s.errorLog.Println(err)
		} else {
			numAffected += int(commantTag.RowsAffected())
		}
	}

	res := &pb.CUD{
		NumAffected: int32(numAffected),
	}

	return res, err
}

func (s *server) GetPriceData(ctx context.Context, req *pb.GetPriceDataRequest) (*pb.PriceData, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

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
		req.InstrumentId, req.StartDateTime.DateTime, req.EndDateTime.DateTime,
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}
	defer query.Close()

	priceData := []*pb.Price{}
	for query.Next() {
		var price pb.Price
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
			price.DateTime = &pb.DateTime{
				DateTime: dateTime.Format(DATETIME_FORMAT),
			}
			priceData = append(priceData, &price)
		}
	}

	res := &pb.PriceData{
		PriceData: priceData,
	}

	return res, nil
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

// func (s *server) InsertTradingSystem(ctx context.Context, req *pb.InsertTradingSystemRequest) (*pb.InsertTradingSystemResponse, error) {
// ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
// defer cancel()

// 	res := &pb.InsertTradingSystemResponse{
// 		Successful: true,
// 	}

// 	return res, nil
// }

// func (s *server) InsertTradingSystemMetrics(ctx context.Context, req *pb.InsertTradingSystemMetricsRequest) (*pb.InsertTradingSystemMetricsResponse, error) {
// ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
// defer cancel()

// 	res := &pb.InsertTradingSystemMetricsResponse{
// 		Successful: true,
// 	}

// 	return res, nil
// }
