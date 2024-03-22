To test the project:

1. Run `go test`


An example of using the project from outside:

```
package main

import (
    imei "github.com/zgordan-vv/dgraph_imei"
)

func main() {
        cli := imei.NewClient("localhost:9080", ":50051")
        if err := cli.ReadXLSXFile("test_file.xlsx"); err != nil {
                log.Fatalf("Failed to parse xlsx file: %v", err)
        }
}
```

You could need to run `go mod init` and `GOPROXY=direct go mod tidy` first
