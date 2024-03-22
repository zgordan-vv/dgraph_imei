package dgraph_imei

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"

	"github.com/xuri/excelize/v2"
	"google.golang.org/grpc"
)

func readXLSXFile(filePath string) ([]*Call, error) {
	conn, err := grpc.Dial(":50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := NewXlsxServiceClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.GetXlsxData(ctx, &GetXlsxRequest{FilePath: filePath})
	if err != nil {
		log.Fatalf("could not fetch XLSX data: %v", err)
	}

	var xlsxData []byte

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("failed to receive a chunk : %v", err)
		}

		xlsxData = append(xlsxData, chunk.Chunk...)
	}

	f, err := excelize.OpenReader(io.NopCloser(bytes.NewReader(xlsxData)))
	if err != nil {
		log.Fatalf("failed to open XLSX data: %v", err)
	}
	defer f.Close()

	rows, err := f.GetRows("Sheet1")
	if err != nil {
		return nil, err
	}

	var data []*Call
	for _, row := range rows[1:] { // Skip header row
		call := &Call{}
		if err := validateStringOfDigits(row[0]); err == nil { // MSDIN
			call.Msdin = row[0]
		} else {
			log.Printf("Invalid MSDIN: %s, %s", row[0], err.Error())
			continue
		}
		if err := validateStringOfDigits(row[1]); err == nil { // IMEI_FROM
			call.ImeiFrom = row[1]
		} else {
			log.Printf("Invalid IMEI_FROM: %s, %s", row[1], err.Error())
			continue
		}
		if lat, err := parseFloat(row[2]); err == nil { // latitude
			call.Latitude = lat
		} else {
			log.Printf("Invalid latitude: %s, %s", row[2], err.Error())
			continue
		}
		if lng, err := parseFloat(row[3]); err == nil { // longitude
			call.Longitude = lng
		} else {
			log.Printf("Invalid longitude: %s, %s", row[3], err.Error())
			continue
		}
		if d, err := parseUnsignedFloat(row[4]); err == nil { // duration
			call.Duration = d
		} else {
			log.Printf("Invalid duration: %s, %s", row[4], err.Error())
			continue
		}
		if err := validateStringOfDigits(row[5]); err == nil { // IMEI_TO
			call.ImeiTo = row[5]
		} else {
			log.Printf("Invalid IMEI_TO: %s, %s", row[5], err.Error())
			continue
		}
		call.CallTime = row[6] // call_time

		data = append(data, call)
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
