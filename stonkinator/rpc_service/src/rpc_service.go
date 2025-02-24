package main

import (
	"context"
	"log"
	"net"
	pb "stonkinator_rpc_service/stonkinator_rpc_service"

	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedStonkinatorServiceServer
}

type service struct {
	infoLog    *log.Logger
	errorLog   *log.Logger
	grpcServer *grpc.Server
	server     *server
}

func (service *service) create() {
	service.grpcServer = grpc.NewServer()
	service.server = &server{}
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

func (s *server) InsertTradingSystem(ctx context.Context, req *pb.InsertTradingSystemRequest) (*pb.InsertTradingSystemResponse, error) {
	res := &pb.InsertTradingSystemResponse{
		Successful: true,
	}

	return res, nil
}

func (s *server) InsertTradingSystemMetrics(ctx context.Context, req *pb.InsertTradingSystemMetricsRequest) (*pb.InsertTradingSystemMetricsResponse, error) {
	res := &pb.InsertTradingSystemMetricsResponse{
		Successful: true,
	}

	return res, nil
}
