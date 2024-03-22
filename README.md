To test the project:

1. Change `GRPC_ADDR` in the `.env` file to your dgraph server URL (`localhost:9080` for a standalone learning environment)
2. Run `go test`


To use the project from outside:

```
import (
    imei as "github.com/zgordan-vv/dgraph_imei"
)
```

and then use `err := imei.ReadXSLXFile(path_to_file)`
