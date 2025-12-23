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
CREATE TABLE IF NOT EXISTS hier_lock_buckets (
  level TINYINT NOT NULL,
  bucket INT NOT NULL,
  PRIMARY KEY (level, bucket)
) ENGINE=InnoDB;
`
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("create table hier_lock_buckets: %v", err)
	}

	// Keep the table small and deterministic per test run.
	if _, err := db.ExecContext(ctx, "TRUNCATE TABLE hier_lock_buckets"); err != nil {
		t.Fatalf("truncate hier_lock_buckets: %v", err)
	}
}

// seedLockKeys is kept only for backward compatibility with earlier iterations.
// The current implementation uses bucket rows in hier_lock_buckets instead.
func seedLockKeys(ctx context.Context, t fataler, db *sql.DB, keys ...string) {
	_ = ctx
	_ = t
	_ = db
	_ = keys
}

func seedBuckets(ctx context.Context, t fataler, db *sql.DB, targets ...lockTarget) {
	for _, tgt := range targets {
		if tgt.bucket < 0 || tgt.bucket >= lockBucketSpace {
			t.Fatalf("bucket out of range: %v", tgt.bucket)
		}
		if _, err := db.ExecContext(ctx,
			"INSERT IGNORE INTO hier_lock_buckets(level, bucket) VALUES (?, ?)",
			int(tgt.level), tgt.bucket,
		); err != nil {
			t.Fatalf("insert bucket row level=%d bucket=%d: %v", tgt.level, tgt.bucket, err)
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
	// keep legacy helper for older tests that still build string keys
	// (not used by bucket-based tests)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, fmt.Sprintf("%d:%d", k.level, k.bucket))
	}
	return out
}

func mustTargets(level Level, userID, accountID, resourceID string) []lockTarget {
	tgts, err := lockKeys(level, userID, accountID, resourceID)
	if err != nil {
		panic(fmt.Sprintf("lockKeys: %v", err))
	}
	return tgts
}

func pickDifferentUserIDNonColliding(baseUserID, accountID, resourceID string) string {
	baseU := userTarget(baseUserID)
	baseA := accountTarget(baseUserID, accountID)
	baseR := resourceTarget(baseUserID, accountID, resourceID)
	for i := 2; ; i++ {
		cand := fmt.Sprintf("%s_%d", baseUserID, i)
		if userTarget(cand) == baseU {
			continue
		}
		if accountTarget(cand, accountID) == baseA {
			continue
		}
		if resourceTarget(cand, accountID, resourceID) == baseR {
			continue
		}
		return cand
	}
}

func pickDifferentAccountIDNonCollidingResource(userID, baseAccountID, resourceID string) string {
	baseA := accountTarget(userID, baseAccountID)
	baseR := resourceTarget(userID, baseAccountID, resourceID)
	for i := 2; ; i++ {
		cand := fmt.Sprintf("%s_%d", baseAccountID, i)
		if accountTarget(userID, cand) == baseA {
			continue
		}
		if resourceTarget(userID, cand, resourceID) == baseR {
			continue
		}
		return cand
	}
}

func pickDifferentResourceID(userID, accountID, baseResourceID string) string {
	baseR := resourceTarget(userID, accountID, baseResourceID)
	for i := 2; ; i++ {
		cand := fmt.Sprintf("%s_%d", baseResourceID, i)
		if resourceTarget(userID, accountID, cand) != baseR {
			return cand
		}
	}
}
