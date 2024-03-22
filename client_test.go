package dgraph_imei

import (
	"log"
	"testing"
)

func TestClient(t *testing.T) {
	go runTestServer() // running test server
	if err := ReadXLSXFile("test_file.xlsx"); err != nil {
		log.Fatalf("Failed to parse xlsx file: %v", err)
	}
}
