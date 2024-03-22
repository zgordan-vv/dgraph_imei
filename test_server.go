package main

import (
	"io"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	port      = ":50051"
	chunkSize = 64 * 1024 // 64 KiB
)

type server struct {
	UnimplementedXlsxServiceServer
}

func (s *server) GetXlsxData(req *GetXlsxRequest, stream XlsxService_GetXlsxDataServer) error {
	filePath := req.GetFilePath()
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	buffer := make([]byte, chunkSize)
	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err := stream.Send(&XlsxDataChunk{Chunk: buffer[:n]}); err != nil {
			return err
		}
	}

	return nil
}

func runTestServer() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	RegisterXlsxServiceServer(s, &server{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

