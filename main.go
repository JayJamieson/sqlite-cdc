package main

import (
	"github.com/JayJamieson/sqlite-cdc/db"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	if len(os.Args) == 1 {
		log.Fatal("provide table and space separated list of tables eg sqlitecdc ./test.db table1 table2 ")
	}

	dbPath := os.Args[1]
	tables := os.Args[2:]

	cdc, err := db.NewSQLiteCDC(dbPath, tables)

	if err != nil {
		log.Fatal(err)
	}

	err = cdc.AddCDC()

	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for event := range cdc.Events {
			log.Printf("%v \n", event)
		}
	}()

	<-termChan

	err = cdc.RemoveCDC()

	if err != nil {
		log.Fatal(err)
	}
}
