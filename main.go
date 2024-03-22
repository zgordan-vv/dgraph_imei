package main

import (
	"log"
	"os"
)

func main() {
	go runTestServer() // running test server
	args := os.Args
	if len(args) < 2 {
		log.Fatal("No filename in command line arguments")
	}
	filename := args[1]
	if err := parseXLSXFile(filename); err != nil {
		log.Fatalf("Failed to parse xlsx file: %v", err)
	}
}
