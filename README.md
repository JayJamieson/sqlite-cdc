# sqlite-cdc (WIP)

sqlite-cdc takes heavy inspiration from [marmot](https://github.com/maxpert/marmot) without all the clustering and replication. 
I wanted all the CDC functionality but simpler. Right now this is a huge work in progress as I work through how I want to enable dispatching
of events.

You can try it out by running command similar to:

```shell
./sqlite-cdc ./test.db table1 table2
```

## Build

- requires CGO_ENABLED=1 for sqlite3

Run `go build main.go`