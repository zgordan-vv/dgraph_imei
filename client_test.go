package dgraph_imei

import (
	"log"
	"testing"
)

func TestClient(t *testing.T) {
	go runTestServer() // running test server
	cli := NewClient("localhost:9080", ":50051")
	if err := cli.ReadXLSXFile("test_file.xlsx"); err != nil {
		log.Fatalf("Failed to parse xlsx file: %v", err)
	}
}
