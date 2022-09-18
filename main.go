package main

import "log"

func main() {
	cdc, err := NewSQLiteCDC("./test.db")

	if err != nil {
		log.Fatal(err)
	}

	cdc.Watch()
}
