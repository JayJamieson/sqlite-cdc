package db

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

// Inspiration for implementation are from
// https://github.com/maxpert/marmot/blob/master/db/change_log.go
// https://github.com/maxpert/marmot/blob/master/db/sqlite.go

var globalChangeTable = "__cdc___change_log_global"
var ScanMaxChanges = 512

type ChangeLogState = int16

const (
	Pending   ChangeLogState = 0
	Published ChangeLogState = 1
	Failed    ChangeLogState = -1
)

type SQLiteCDC struct {
	// TODO probably get rid of goqu if not fully utilized
	*goqu.Database
	conn              *sql.DB
	rawConnection     *sqlite3.SQLiteConn
	watcher           *fsnotify.Watcher
	dbPath            string
	lastId            uint64
	watchTablesSchema map[string][]*sql.ColumnType
	tables            []string
	Events            chan any
}

type globalChangeLogEntry struct {
	Id            int64  `db:"id"`
	ChangeTableId int64  `db:"change_table_id"`
	TableName     string `db:"table_name"`
}

type changeLogEntry struct {
	Id    int64  `db:"id"`
	Type  string `db:"type"`
	State string `db:"state"`
}

func NewSQLiteCDC(path string, tables []string) (*SQLiteCDC, error) {
	dsn := fmt.Sprintf("%s?_journal_mode=WAL", path)

	conn, rawConn, err := open(dsn)
	if err != nil {
		return nil, err
	}

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
		Database:          sqliteQu.DB(conn),
		conn:              conn,
		rawConnection:     rawConn,
		watcher:           watcher,
		dbPath:            path,
		lastId:            0,
		watchTablesSchema: map[string][]*sql.ColumnType{},
		tables:            tables,
		Events:            make(chan any),
	}
	return ret, nil
}

// AddCDC creates single global log table and per configured log table. Creates
// triggers to update each log tables of changes.
func (cdc *SQLiteCDC) AddCDC() error {
	sqlConn := cdc.conn
	tables := cdc.tables
	tx, err := sqlConn.Begin()

	if err != nil {
		return err
	}

	err = withTx(tx, func(tx *sql.Tx) error {
		for _, n := range tables {
			colInfo, err := getTableInfo(tx, n)
			if err != nil {
				return err
			}
			cdc.watchTablesSchema[n] = colInfo
		}
		return nil
	})

	if err != nil {
		return err
	}

	return cdc.installChangeLogTriggers()
}

func (cdc *SQLiteCDC) RemoveCDC() error {
	sqlConn := cdc.conn
	tables := cdc.tables
	tx, err := sqlConn.Begin()

	if err != nil {
		return err
	}

	err = withTx(tx, func(tx *sql.Tx) error {
		log.Println("removing global log table")

		_, err := tx.Exec("DROP TABLE IF EXISTS __cdc___change_log_global;")

		if err != nil {
			log.Println("failed removing table")
			return err
		}

		for _, table := range tables {
			log.Printf("removing %s trigger\n", table)
			for _, trigger := range []string{"insert", "delete", "update"} {
				_, err := tx.Exec(fmt.Sprintf("DROP TRIGGER IF EXISTS __cdc__%s_change_log_on_%s;", table, trigger))

				if err != nil {
					log.Println("failed removing trigger", table)
					return err
				}
			}

			log.Printf("removing table %s \n", table)
			_, err := tx.Exec(fmt.Sprintf("DROP TABLE IF EXISTS __cdc__%s_change_log;", table))

			if err != nil {
				log.Println("failed removing table", table)
				return err
			}
		}
		return nil
	})

	return err
}

// getTableInfo Does a limit 1 query on specified table to get []*sql.ColumnType
// for each column on the table. This allows building a log table and triggers from
// original table
func getTableInfo(tx *sql.Tx, table string) ([]*sql.ColumnType, error) {
	queryResult, err := tx.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 1", table))
	if err != nil {
		return nil, err
	}

	types, err := queryResult.ColumnTypes()

	if err != nil {
		return nil, err
	}
	return types, nil
}

func open(dsn string) (*sql.DB, *sqlite3.SQLiteConn, error) {
	var rawConn *sqlite3.SQLiteConn
	d := &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			rawConn = conn
			return conn.RegisterFunc("cdc_version", func() string {
				return "0.1"
			}, true)
		},
	}

	conn := sql.OpenDB(Connector{driver: d, dns: dsn})

	err := conn.Ping()

	conn.SetConnMaxLifetime(0)
	conn.SetConnMaxIdleTime(10 * time.Second)

	if err != nil {
		return nil, nil, err
	}

	return conn, rawConn, nil
}

func (cdc *SQLiteCDC) Watch() {
	shmPath := cdc.dbPath + "-shm"
	walPath := cdc.dbPath + "-wal"
	watcher := cdc.watcher

	errShm := watcher.Add(shmPath)
	errWal := watcher.Add(walPath)

	for {
		select {
		case ev, ok := <-cdc.watcher.Events:
			if !ok {
				log.Println("event channel closed")
				return
			}
			if ev.Op != fsnotify.Chmod {
				var entries []globalChangeLogEntry

				tx, err := cdc.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})

				if err != nil {
					log.Printf("problem starting read transaction %v\n", err)
					return
				}

				err = tx.From(globalChangeTable).
					Order(goqu.I("id").Asc()).
					Where(goqu.C("id").Gt(cdc.lastId)).
					Limit(uint(ScanMaxChanges)).
					Prepared(true).
					ScanStructs(&entries)

				fetchFail := false

				if err != nil {
					log.Println("failed fetching rows", err)
					fetchFail = true
				}

				err = tx.Rollback()

				if err != nil || fetchFail {
					log.Println("failed read transction", err)
					continue
				}

				if len(entries) == 0 {
					log.Println("found no changes", len(entries))
					continue
				}

				for _, entry := range entries {
					cdc.Events <- entry
				}

				lastChange, err := lo.Last(entries)

				if err != nil {
					log.Println("failed getting last item in result set", err)
					continue
				}

				cdc.lastId = uint64(lastChange.Id)

			}
			log.Println("Last Id set", cdc.lastId)

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

func (cdc *SQLiteCDC) installChangeLogTriggers() error {
	for tableName, columnTypes := range cdc.watchTablesSchema {
		log.Printf("Adding trigger to table %s \n", tableName)
		err := cdc.initTriggers(tableName, columnTypes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cdc *SQLiteCDC) initTriggers(table string, columns []*sql.ColumnType) error {
	logSql, err := buildLogSql(table, columns)

	if err != nil {
		log.Println(err)
		return err
	}

	triggerSql, err := buildTriggerSql(table, columns)

	if err != nil {
		log.Println(err)
		return err
	}
	log.Printf("Adding %s log table\n", table)
	_, err = cdc.conn.Exec(logSql)

	if err != nil {
		log.Println(err)
		return err
	}

	log.Printf("Adding %s triggers\n", table)
	_, err = cdc.conn.Exec(triggerSql)

	if err != nil {
		log.Println(err)
		return err
	}

	log.Println("Successfully added log table and triggers")

	return nil
}

func withTx(tx *sql.Tx, fn func(tx *sql.Tx) error) error {
	return wrapTx(tx, func() error { return fn(tx) })
}

func wrapTx(tx *sql.Tx, fn func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				err = rollbackErr
			}
		} else {
			if commitErr := tx.Commit(); commitErr != nil {
				err = commitErr
			}
		}
	}()
	return fn()
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
