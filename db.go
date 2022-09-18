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
	"github.com/samber/lo"
)

type SQLiteCDC struct {
	*goqu.Database
	rawConnection *sqlite3.SQLiteConn
	dbPath        string
	watcher       *fsnotify.Watcher
	lastId        uint64
}

type userLog struct {
	Id    int64  `db:"id"`
	Type  string `db:"type"`
	State string `db:"state"`
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
	log.Println(path)

	if err != nil {
		return nil, err
	}

	sqliteQu := goqu.Dialect("sqlite3")
	ret := &SQLiteCDC{
		Database:      sqliteQu.DB(conn),
		rawConnection: rawConn,
		watcher:       watcher,
		dbPath:        path,
		lastId:        0,
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
				log.Println("event channel closed")
				return
			}
			if ev.Op != fsnotify.Chmod {
				var changes []*userLog
				err := connection.Select().
					From("__marmot__artist_change_log").
					Where(goqu.C("id").Gt(connection.lastId)).
					Prepared(true).
					ScanStructs(&changes)
					// Order(goqu.I("artistId").Asc()).

				if err != nil {
					log.Println("failed fetching rows", err)
					continue
				}

				if len(changes) == 0 {
					log.Println("found no changes", len(changes))
					continue
				}

				for _, artist := range changes {
					log.Println("Got new artist with id and name ", artist.Id, artist.Type)
				}
				lastChange, err := lo.Last(changes)

				if err != nil {
					log.Println("failed getting last item in result set", err)
					continue
				}

				connection.lastId = uint64(lastChange.Id)
			}
			log.Println("Last Id set", connection.lastId)
		case <-time.After(time.Second * 5):

			if errShm != nil {
				log.Println("errShm", errShm)
				errShm = watcher.Add(shmPath)
			}

			if errWal != nil {
				log.Println("errWal", errWal)
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
