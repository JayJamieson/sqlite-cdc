package db

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/andreyvit/diff"
	"github.com/matryer/is"
)

func setupDb(tableName string) (*sql.DB, error) {
	conn, _, _ := open(":memory:")
	_, err := conn.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s
	(
    	id INTEGER PRIMARY KEY AUTOINCREMENT,
    	t  TEXT,
    	nu NUMERIC,
    	i  INTEGER,
    	r  REAL,
    	no BLOB
	);`, tableName))

	return conn, err
}

func TestTable_buildLogSql(t *testing.T) {
	assert := is.New(t)
	conn, err := setupDb("test")

	if err != nil {
		t.Error(err)
	}

	var expected = `CREATE TABLE IF NOT EXISTS __cdc___change_log_global (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    change_table_id INTEGER,
    table_name TEXT
    );
    CREATE TABLE IF NOT EXISTS __cdc__test_change_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    val_id INTEGER,
    val_t TEXT,
    val_nu NUMERIC,
    val_i INTEGER,
    val_r REAL,
    val_no BLOB,
    type TEXT,
    created_at INTEGER,
    state INTEGER
    );
    CREATE INDEX IF NOT EXISTS __cdc__test_change_log_state_index ON __cdc__test_change_log (state);
    `

	rows, _ := conn.Query("SELECT * FROM test LIMIT 1")
	cols, _ := rows.ColumnTypes()

	tableSql, _ := buildLogSql("test", cols)

	if a, e := strings.TrimSpace(tableSql), strings.TrimSpace(expected); a != e {
		t.Errorf("Result not as expected:\n%v", diff.LineDiff(e, a))
	}

	assert.Equal(tableSql, expected)
}

func TestTable_buildTriggerSql(t *testing.T) {
	assert := is.New(t)

	conn, err := setupDb("test")

	rows, _ := conn.Query("SELECT * FROM test LIMIT 1")
	cols, _ := rows.ColumnTypes()

	if err != nil {
		t.Error(err)
	}

	expected := `DROP TRIGGER IF EXISTS __cdc__test_change_log_on_delete;
    CREATE TRIGGER IF NOT EXISTS __cdc__test_change_log_on_delete
    AFTER delete ON test
    BEGIN
    INSERT INTO __cdc__test_change_log(
    val_id,
    val_t,
    val_nu,
    val_i,
    val_r,
    val_no,
    type,
    created_at,
    state
    ) VALUES(
    OLD.id,
    OLD.t,
    OLD.nu,
    OLD.i,
    OLD.r,
    OLD.no,
    'delete',
    CAST((strftime('%s','now') || substr(strftime('%f','now'),4)) as INT),
    0 -- Pending
    );
    INSERT INTO __cdc___change_log_global (change_table_id, table_name)
    VALUES (
    last_insert_rowid(),
    'test'
    );
    END;
    DROP TRIGGER IF EXISTS __cdc__test_change_log_on_insert;
    CREATE TRIGGER IF NOT EXISTS __cdc__test_change_log_on_insert
    AFTER insert ON test
    BEGIN
    INSERT INTO __cdc__test_change_log(
    val_id,
    val_t,
    val_nu,
    val_i,
    val_r,
    val_no,
    type,
    created_at,
    state
    ) VALUES(
    NEW.id,
    NEW.t,
    NEW.nu,
    NEW.i,
    NEW.r,
    NEW.no,
    'insert',
    CAST((strftime('%s','now') || substr(strftime('%f','now'),4)) as INT),
    0 -- Pending
    );
    INSERT INTO __cdc___change_log_global (change_table_id, table_name)
    VALUES (
    last_insert_rowid(),
    'test'
    );
    END;
    DROP TRIGGER IF EXISTS __cdc__test_change_log_on_update;
    CREATE TRIGGER IF NOT EXISTS __cdc__test_change_log_on_update
    AFTER update ON test
    BEGIN
    INSERT INTO __cdc__test_change_log(
    val_id,
    val_t,
    val_nu,
    val_i,
    val_r,
    val_no,
    type,
    created_at,
    state
    ) VALUES(
    NEW.id,
    NEW.t,
    NEW.nu,
    NEW.i,
    NEW.r,
    NEW.no,
    'update',
    CAST((strftime('%s','now') || substr(strftime('%f','now'),4)) as INT),
    0 -- Pending
    );
    INSERT INTO __cdc___change_log_global (change_table_id, table_name)
    VALUES (
    last_insert_rowid(),
    'test'
    );
    END;
    `

	triggerSql, _ := buildTriggerSql("test", cols)

	if a, e := strings.TrimSpace(triggerSql), strings.TrimSpace(expected); a != e {
		t.Errorf("Result not as expected:\n%v", diff.LineDiff(e, a))
	}

	assert.Equal(triggerSql, expected)
}
