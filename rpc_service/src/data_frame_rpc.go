package main

import (
	"context"
	pb "stonkinator_rpc_service/stonkinator_rpc_service"
)

func (s *server) PushPrice(req *pb.Price) (*pb.CUD, error) {
	cud, err := s.dfServiceClient.PushPrice(context.Background(), req)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	return cud, err
}

func (s *server) PushPriceStream(priceData []*pb.Price) (*pb.CUD, error) {
	stream, err := s.dfServiceClient.PushPriceStream(context.Background())
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	for _, price := range priceData {
		if err := stream.Send(price); err != nil {
			s.errorLog.Println(err)
			return nil, err
		}
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	return res, err
}
