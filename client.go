package dgraph_imei

import (
	"log"

	"github.com/dgraph-io/dgo/v230"
	"github.com/dgraph-io/dgo/v230/protos/api"
	"google.golang.org/grpc"
)

type FileClient struct {
	dgraphClient   *dgo.Dgraph
	grpcServerAddr string
}

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

func NewClient(dgraphGRPCAddr, grpcServerAddr string) *FileClient {
	dc := newDgraphClient(dgraphGRPCAddr)
	client := &FileClient{
		dgraphClient: dc,
		grpcServerAddr: grpcServerAddr,
	}
	return client
}

// ReadXLSXFile reads an xlsx file with a given name or path from GRPC server
func (c *FileClient) ReadXLSXFile(filename string) error {
	data, err := readXLSXFile(filename, c.grpcServerAddr)
	if err != nil {
		return err
	}

	for _, call := range data {
		err = upsertAll(c.dgraphClient, call)
		if err != nil {
			return err
		}
	}
	return nil
}

func newDgraphClient(dgraphGRPCAddr string) *dgo.Dgraph {
	dialOpts := []grpc.DialOption{grpc.WithInsecure()}
	conn, err := grpc.Dial(dgraphGRPCAddr, dialOpts...)
	if err != nil {
		log.Fatalf("Cannot dial Dgraph client: %v", err)
	}

	return dgo.NewDgraphClient(
		api.NewDgraphClient(conn),
	)
}
