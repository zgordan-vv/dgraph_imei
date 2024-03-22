package main

import (
	"log"
	"os"

	"github.com/dgraph-io/dgo/v230"
	"github.com/dgraph-io/dgo/v230/protos/api"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

type Call struct {
	Msdin	   string  `json:"MSDIN"`
	ImeiFrom   string  `json:"IMEI_FROM"`
	Latitude   float64 `json:"latitude"`
	Longitude  float64 `json:"longitude"`
	Duration   float64 `json:"duration"`
	ImeiTo	   string  `json:"IMEI_TO"`
	CallTime   string  `json:"call_time"`
	DgraphType string  `json:"dgraph.type"`
}

func parseXLSXFile(filename string) error {
	data, err := readXLSXFile(filename)
	if err != nil {
		return err
	}

	client := newDgraphClient()

	for _, call := range data {
		err = upsertAll(client, call)
		if err != nil {
			return err
		}
	}
	return nil
}

func newDgraphClient() *dgo.Dgraph {
	if err := godotenv.Load(".env"); err != nil{
		log.Fatalf("Error loading .env file: %s", err)
	}
	grpcAddr := os.Getenv("GRPC_ADDR")
	dialOpts := []grpc.DialOption{grpc.WithInsecure()}
	conn, err := grpc.Dial(grpcAddr, dialOpts...)
	if err != nil {
		log.Fatalf("Cannot dial Dgraph client: %v", err)
	}

	return dgo.NewDgraphClient(
		api.NewDgraphClient(conn),
	)
}
