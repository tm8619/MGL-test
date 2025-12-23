package hierlock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
)

func openTestDB(t fataler) (*sql.DB, func()) {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		// docker-compose.yml defaults
		dsn = "testuser:testpassword@tcp(127.0.0.1:3306)/mgl_test?parseTime=true&multiStatements=true"
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	// Make sure we can get distinct sessions for concurrent transactions.
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(2 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Skipf("MySQL not reachable (set MYSQL_DSN or run docker-compose up -d): %v", err)
	}

	cleanup := func() {
		_ = db.Close()
	}
	return db, cleanup
}

type fataler interface {
	Fatalf(format string, args ...any)
	Skipf(format string, args ...any)
}

func setupLockTable(ctx context.Context, t fataler, db *sql.DB) {
	stmt := `
CREATE TABLE IF NOT EXISTS hier_locks (
  lock_key VARCHAR(255) NOT NULL,
  PRIMARY KEY (lock_key)
) ENGINE=InnoDB;
`
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("create table hier_locks: %v", err)
	}

	// Keep the table small and deterministic per test run.
	if _, err := db.ExecContext(ctx, "TRUNCATE TABLE hier_locks"); err != nil {
		t.Fatalf("truncate hier_locks: %v", err)
	}
}

func seedLockKeys(ctx context.Context, t fataler, db *sql.DB, keys ...string) {
	for _, k := range keys {
		if k == "" {
			t.Fatalf("empty lock key")
		}
		// INSERT IGNORE is safe for idempotent seeding.
		if _, err := db.ExecContext(ctx, "INSERT IGNORE INTO hier_locks(lock_key) VALUES (?)", k); err != nil {
			t.Fatalf("insert lock key %q: %v", k, err)
		}
	}
}

func isLockConflict(err error) bool {
	var me *mysql.MySQLError
	if ok := asMySQLError(err, &me); !ok {
		return false
	}
	// Some environments may surface timeout/deadlock depending on server settings.
	switch me.Number {
	case 1205, 1213:
		return true
	default:
		return false
	}
}

func isDeadlock(err error) bool {
	var me *mysql.MySQLError
	if !errors.As(err, &me) {
		return false
	}
	return me.Number == 1213
}

func isLockWaitTimeout(err error) bool {
	var me *mysql.MySQLError
	if !errors.As(err, &me) {
		return false
	}
	return me.Number == 1205
}

func asMySQLError(err error, target **mysql.MySQLError) bool {
	// Avoid importing errors package in multiple files; keep it local and explicit.
	tmp := err
	for tmp != nil {
		if e, ok := tmp.(*mysql.MySQLError); ok {
			*target = e
			return true
		}
		// Unwrap if possible
		u, ok := tmp.(interface{ Unwrap() error })
		if !ok {
			break
		}
		tmp = u.Unwrap()
	}
	return false
}

func mustKeys(level Level, userID, accountID, resourceID string) []string {
	keys, err := lockKeys(level, userID, accountID, resourceID)
	if err != nil {
		panic(fmt.Sprintf("lockKeys: %v", err))
	}
	return keys
}
