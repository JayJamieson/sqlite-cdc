# sqlite-cdc

CDC Events with SQLite

```sql
CREATE TABLE artist
(
    artistId INTEGER NOT NULL,
    name     NVARCHAR(120),
    CONSTRAINT [PK_Artist] PRIMARY KEY (artistId)
);

insert into artist (artistId, name) values (1, 'test');

DROP TABLE IF EXISTS __marmot__artist_change_log;
CREATE TABLE IF NOT EXISTS __marmot__artist_change_log
(
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    val_artistId INTEGER NOT NULL,
    val_name     NVARCHAR(120),
    type         TEXT,
    created_at   INTEGER,
    state        INTEGER
);
CREATE INDEX IF NOT EXISTS __marmot__artist_change_log_state_index ON __marmot__artist_change_log (state);
DROP TRIGGER IF EXISTS __marmot__artist_change_log_on_delete;

CREATE TRIGGER IF NOT EXISTS __marmot__artist_change_log_on_delete
    AFTER delete
    ON artist
BEGIN
    INSERT INTO __marmot__artist_change_log(val_artistId,
                                            val_name,
                                            type,
                                            created_at,
                                            state)
    VALUES (OLD.artistId,
            OLD.name,
            'delete',
            CAST((strftime('%s', 'now') || substr(strftime('%f', 'now'), 4)) as INT),
            0 -- Pending
           );
END;
DROP TRIGGER IF EXISTS __marmot__artist_change_log_on_insert;
CREATE TRIGGER IF NOT EXISTS __marmot__artist_change_log_on_insert
    AFTER insert
    ON artist
BEGIN
    INSERT INTO __marmot__artist_change_log(val_artistId, val_name, type, created_at, state)
    VALUES (NEW.artistId, NEW.name, 'insert', CAST((strftime('%s', 'now') || substr(strftime('%f', 'now'), 4)) as INT),
            0 -- Pending
           );
END;

DROP TRIGGER IF EXISTS __marmot__artist_change_log_on_update;
CREATE TRIGGER IF NOT EXISTS __marmot__artist_change_log_on_update
    AFTER update
    ON artist
BEGIN
    INSERT INTO __marmot__artist_change_log(val_artistId, val_name, type, created_at, state)
    VALUES (NEW.artistId, NEW.name, 'update', CAST((strftime('%s', 'now') || substr(strftime('%f', 'now'), 4)) as INT),
            0 -- Pending
           );
END;
```
