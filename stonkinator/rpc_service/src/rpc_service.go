package main

import (
	"context"
	"log"
	"net"
	pb "stonkinator_rpc_service/stonkinator_rpc_service"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

// const dbTimeout = time.Second * 6

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

func (service *service) run() error {
	listener, err := net.Listen("tcp", ":5000")
	if err != nil {
		return err
	}

	if err := service.grpcServer.Serve(listener); err != nil {
		return err
	}

	return nil
}

func (s *server) InsertExchange(ctx context.Context, req *pb.InsertExchangeRequest) (*pb.InsertResponse, error) {
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

	successful := true
	if err != nil {
		// TODO: Respond with, or log errors, define centralized logging m
		successful = false
	}
	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		successful = false
	}

	res := &pb.InsertResponse{
		Successful: successful,
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

	var id string
	var exchangeName string
	// var res *pb.GetExchangeResponse
	successful := true
	err := query.Scan(&id, &exchangeName)
	// err := query.Scan(&res.Id, &res.ExchangeName)
	if err != nil {
		successful = false
		res := &pb.GetExchangeResponse{
			Successful: successful,
		}
		return res, err
	}

	res := &pb.GetExchangeResponse{
		Id:           id,
		ExchangeName: exchangeName,
		Successful:   successful,
	}
	// res.Successful = successful

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

	successful := true
	if err != nil {
		successful = false
	}
	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		successful = false
	}

	res := &pb.InsertResponse{
		Successful: successful,
	}

	return res, nil
}

func (s *server) GetInstrument(ctx context.Context, req *pb.GetBySymbolRequest) (*pb.Instrument, error) {
	query := s.pgPool.QueryRow(
		context.Background(),
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
		// successful = false
	}

	return res, nil
}

func (s *server) InsertPriceData(ctx context.Context, req *pb.InsertPriceDataRequest) (*pb.InsertResponse, error) {
	res := &pb.InsertResponse{}

	return res, nil
}

func (s *server) GetPriceData(ctx context.Context, req *pb.GetPriceDataRequest) (*pb.GetPriceDataResponse, error) {
	res := &pb.GetPriceDataResponse{}

	return res, nil
}

func (s *server) GetDateTime(ctx context.Context, req *pb.GetDateTimeRequest) (*pb.DateTime, error) {
	res := &pb.DateTime{}

	return res, nil
}

func (s *server) GetLastDate(ctx context.Context, req *pb.GetLastDateRequest) (*pb.DateTime, error) {
	res := &pb.DateTime{}

	return res, nil
}

func (s *server) GetMarketListInstruments(ctx context.Context, req *pb.MarketList) (*pb.Instruments, error) {
	res := &pb.Instruments{}

	return res, nil
}

func (s *server) GetMarketListInstrumentsPriceData(ctx context.Context, req *pb.MarketList) (*pb.GetPriceDataResponse, error) {
	res := &pb.GetPriceDataResponse{}

	return res, nil
}

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
