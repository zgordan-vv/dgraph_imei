package main

import (
	"fmt"
	"log"

	"github.com/dgraph-io/dgo/v230"
	"github.com/dgraph-io/dgo/v230/protos/api"
	"google.golang.org/grpc"
)

const grpcAddr = "localhost:9080"

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

func main() {
	data, err := readXLSXFile("./test_file.xlsx")
	if err != nil {
		log.Fatalf("Error reading .xlsx file %v", err)
	}

	client := newDgraphClient()

	for _, call := range data {
		fmt.Println("\n\n\nUPSERTING:", call)
		err = upsertAll(client, call)
		if err != nil {
			log.Fatalf("Failed to upsert data: %v", err)
		}
	}
}

func newDgraphClient() *dgo.Dgraph {
	dialOpts := []grpc.DialOption{grpc.WithInsecure()}
	conn, err := grpc.Dial(grpcAddr, dialOpts...)
	if err != nil {
		log.Fatal(err)
	}

	return dgo.NewDgraphClient(
		api.NewDgraphClient(conn),
	)
}
