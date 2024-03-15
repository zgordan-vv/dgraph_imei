package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/dgraph-io/dgo/v230"
	"github.com/dgraph-io/dgo/v230/protos/api"
	"github.com/xuri/excelize/v2"
	"google.golang.org/grpc"
)

const grpcAddr = "localhost:9080"

type Record struct {
	Msdin	 string
	ImeiFrom  string
	Latitude  float64
	Longitude float64
	Duration  float64
	ImeiTo	string
}

func main() {
	data, err := readXLSXFile("./test_file.xlsx")
	if err != nil {
		log.Fatalf("Error reading .xlsx file %v", err)
	}

	newSchema := `
		IMEI_FROM: string @index(exact) .
		IMEI_TO: string .
		MSDIN: string @index(exact) .
		latitude: float .
		longitude: float .
		duration: float .
		timestamp: datetime @index(day) .
		device_to_account: uid @reverse .
		account_to_device: uid .
	`

	client := newDgraphClient()

	if err := alterSchema(client, newSchema); err != nil {
		log.Fatalf("Error altering schema: %v", err)
	}

	err = insertIntoDgraph(client, data)
	if err != nil {
		log.Fatalf("Failed to upsert data: %v", err)
	}
}

func alterSchema(client *dgo.Dgraph, schema string) error {
	ctx := context.Background()
	op := &api.Operation{Schema: schema}
	if err := client.Alter(ctx, op); err != nil {
		return fmt.Errorf("failed to alter schema: %w", err)
	}
	return nil
}

func readXLSXFile(filePath string) ([]*Record, error) {
	f, err := excelize.OpenFile(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	rows, err := f.GetRows("Sheet1")
	if err != nil {
		return nil, err
	}

	var data []*Record
	for _, row := range rows[1:] { // Skip header row
		record := &Record{}
		if err := validateStringOfDigits(row[0]); err == nil { // MSDIN
			record.Msdin = row[0]
		} else {
			log.Printf("Invalid MSDIN: %s, %s", row[0], err.Error())
			continue
		}
		if err := validateStringOfDigits(row[1]); err == nil { // IMEI_FROM
			record.ImeiFrom = row[1]
		} else {
			log.Printf("Invalid IMEI_FROM: %s, %s", row[1], err.Error())
			continue
		}
		if lat, err := parseFloat(row[2]); err == nil { // latitude
			record.Latitude = lat
		} else {
			log.Printf("Invalid latitude: %s, %s", row[2], err.Error())
			continue
		}
		if lng, err := parseFloat(row[3]); err == nil { // longitude
			record.Longitude = lng
		} else {
			log.Printf("Invalid longitude: %s, %s", row[3], err.Error())
			continue
		}
		if d, err := parseUnsignedFloat(row[4]); err == nil { // duration
			record.Duration = d
		} else {
			log.Printf("Invalid duration: %s, %s", row[4], err.Error())
			continue
		}
		if err := validateStringOfDigits(row[5]); err == nil { // IMEI_TO
			record.ImeiTo = row[5]
		} else {
			log.Printf("Invalid IMEI_TO: %s, %s", row[5], err.Error())
			continue
		}

		data = append(data, record)
	}
	return data, nil
}

func validateStringOfDigits(str string) error {
	if len(str) == 0  {
		return errors.New("The string is empty")
	}
	if _, err := strconv.ParseInt(str, 10, 64); err != nil {
		return err
	}
	return nil
}

func parseUnsignedFloat(str string) (float64, error) {
	fl, err := parseFloat(str)
	if err != nil {
		return 0, err
	}
	if fl < 0 {
		return 0, fmt.Errorf("float number is negative: %v", fl)
	}
	return fl, nil
}

func parseFloat(str string) (float64, error) {
	return strconv.ParseFloat(str, 64)
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

func insertIntoDgraph(client *dgo.Dgraph, data []*Record) error {
    ctx := context.Background()

    for _, record := range data {
        timestamp := time.Now().Format(time.RFC3339)

        mutation := &api.Mutation{
            SetNquads: []byte(fmt.Sprintf(`
                _:device <IMEI_FROM> "%s" .
                _:device <latitude> "%f" .
                _:device <longitude> "%f" .
                _:device <duration> "%f" .
                _:device <IMEI_TO> "%s" .
                _:device <MSDIN> "%s" .
                _:device <timestamp> "%s" .
                _:device <dgraph.type> "Device" .

                _:account <MSDIN> "%s" .
                _:account <IMEI_FROM> "%s" .
                _:account <latitude> "%f" .
                _:account <longitude> "%f" .
                _:account <duration> "%f" .
                _:account <IMEI_TO> "%s" .
                _:account <timestamp> "%s" .
                _:account <dgraph.type> "Account" .

                _:device <device_to_account> _:account .
                _:account <account_to_device> _:device .`,
                record.ImeiFrom, record.Latitude, record.Longitude, record.Duration, record.ImeiTo, record.Msdin, timestamp,
                record.Msdin, record.ImeiFrom, record.Latitude, record.Longitude, record.Duration, record.ImeiTo, timestamp)),
        }

	txn := client.NewTxn()
	defer txn.Discard(ctx)
        if _, err := txn.Mutate(ctx, mutation); err != nil {
            log.Printf("Failed to mutate record for IMEI_FROM %s with timestamp %s: %v", record.ImeiFrom, timestamp, err)
            continue
        }
	if err := txn.Commit(ctx); err != nil {
            log.Printf("Failed to commit record for IMEI_FROM %s with timestamp %s: %v", record.ImeiFrom, timestamp, err)
            continue
	}
    }

    return nil
}
