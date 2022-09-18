package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/fsnotify/fsnotify"
	"github.com/mattn/go-sqlite3"
)

type SQLiteCDC struct {
	*goqu.Database
	rawConnection *sqlite3.SQLiteConn
	dbPath        string
	watcher       *fsnotify.Watcher
}

func NewSQLiteCDC(path string) (*SQLiteCDC, error) {
	connectionStr := fmt.Sprintf("%s?_journal_mode=wal", path)
	log.Println(connectionStr)

	conn, rawConn, err := Open(connectionStr)
	if err != nil {
		return nil, err
	}

	conn.SetConnMaxLifetime(0)
	conn.SetConnMaxIdleTime(10 * time.Second)
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	err = watcher.Add(path)
	if err != nil {
		return nil, err
	}

	sqliteQu := goqu.Dialect("sqlite3")
	ret := &SQLiteCDC{
		Database:      sqliteQu.DB(conn),
		rawConnection: rawConn,
		watcher:       watcher,
		dbPath:        path,
	}

	return ret, nil
}

func Open(connStr string) (*sql.DB, *sqlite3.SQLiteConn, error) {
	var rawConn *sqlite3.SQLiteConn
	d := &sqlite3.SQLiteDriver{}

	conn := sql.OpenDB(Connector{driver: d, dns: connStr})
	err := conn.Ping()
	if err != nil {
		return nil, nil, err
	}

	return conn, rawConn, nil
}

func (connection *SQLiteCDC) Watch() {
	shmPath := connection.dbPath + "-shm"
	walPath := connection.dbPath + "-wal"
	watcher := connection.watcher

	errShm := watcher.Add(shmPath)
	errWal := watcher.Add(walPath)

	for {
		select {
		case ev, ok := <-connection.watcher.Events:
			if !ok {
				return
			}

			if ev.Op != fsnotify.Chmod {
				log.Println("Event:", ev)
			}
		case <-time.After(time.Second * 5):
			if errShm != nil {
				errShm = watcher.Add(shmPath)
			}

			if errWal != nil {
				errWal = watcher.Add(walPath)
			}
		}
	}
}

type Connector struct {
	driver driver.Driver
	dns    string
}

func (t Connector) Connect(_ context.Context) (driver.Conn, error) {
	return t.driver.Open(t.dns)
}

func (t Connector) Driver() driver.Driver {
	return t.driver
}
