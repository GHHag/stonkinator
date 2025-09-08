package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	pb "stonkinator_rpc_service/stonkinator_rpc_service"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const DB_TIMEOUT = time.Second * 20
const DATE_FORMAT = "2006-01-02"
const DATE_TIME_FORMAT = "2006-01-02 15:04:05"
const MAX_MESSAGE_SIZE = 4 * 1024 * 1024

type server struct {
	infoLog         *log.Logger
	errorLog        *log.Logger
	pgPool          *pgxpool.Pool
	dfServiceClient pb.DataFrameServiceClient
	pb.UnimplementedSecuritiesServiceServer
	pb.UnimplementedTradingSystemsServiceServer
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

	dfClientConn, err := grpc.NewClient("data_frame_service:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		service.errorLog.Println(err)
		return err
	}

	go func() {
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
		<-signalChan
		defer dfClientConn.Close()
	}()

	dfServiceClient := pb.NewDataFrameServiceClient(dfClientConn)

	// If all services are inside the same network, you can use local credentials for lightweight security.
	// creds := grpc.LocalCredentials(grpc.LocalConnectionType.UDS)

	service.grpcServer = grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsConfig)),
		grpc.KeepaliveParams(keepAliveParams),
		grpc.MaxRecvMsgSize(MAX_MESSAGE_SIZE),
		grpc.MaxSendMsgSize(MAX_MESSAGE_SIZE),
	)
	service.server = &server{
		infoLog:         service.infoLog,
		errorLog:        service.errorLog,
		pgPool:          pgPool,
		dfServiceClient: dfServiceClient,
	}
	pb.RegisterSecuritiesServiceServer(service.grpcServer, service.server)
	pb.RegisterTradingSystemsServiceServer(service.grpcServer, service.server)

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
