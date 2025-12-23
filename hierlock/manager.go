package hierlock

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"sort"
)

const lockBucketSpace = 10_000_000

type Level int

const (
	LevelUser Level = iota
	LevelAccount
	LevelResource
)

type LockHandle struct {
	tx *sql.Tx
}

// Release releases all row locks by rolling back the underlying transaction.
// (We intentionally rollback because this is a pure lock acquisition transaction.)
func (h *LockHandle) Release() error {
	if h == nil || h.tx == nil {
		return nil
	}
	return h.tx.Rollback()
}

type Manager struct {
	db *sql.DB
}

func NewManager(db *sql.DB) *Manager {
	return &Manager{db: db}
}

// Acquire locks the hierarchy using MySQL row locks.
//
// Rule:
// - ancestors: shared lock (FOR SHARE)
// - target: exclusive lock (FOR UPDATE)
//
// The locks are held until LockHandle.Release() (tx rollback).
func (m *Manager) Acquire(ctx context.Context, level Level, userID, accountID, resourceID string) (*LockHandle, error) {
	if m == nil || m.db == nil {
		return nil, fmt.Errorf("manager db is nil")
	}
	keys, err := lockKeys(level, userID, accountID, resourceID)
	if err != nil {
		return nil, err
	}

	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}

	// If anything fails, rollback to release any acquired locks.
	rollback := func(cause error) (*LockHandle, error) {
		_ = tx.Rollback()
		return nil, cause
	}

	// Acquire in strict ancestor->descendant order to avoid deadlocks.
	for i, key := range keys {
		isTarget := i == len(keys)-1
		if err := lockRow(ctx, tx, key, isTarget); err != nil {
			return rollback(err)
		}
	}

	return &LockHandle{tx: tx}, nil
}

// AcquireResources locks a fixed hierarchy (User -> Account -> Resources...).
//
// Rule:
// - User, Account: shared lock
// - Each Resource: exclusive lock
//
// Resources are locked in lexicographical order to avoid deadlocks when multiple
// transactions lock multiple resources.
func (m *Manager) AcquireResources(ctx context.Context, userID, accountID string, resourceIDs []string) (*LockHandle, error) {
	if m == nil || m.db == nil {
		return nil, fmt.Errorf("manager db is nil")
	}
	if userID == "" || accountID == "" {
		return nil, fmt.Errorf("userID and accountID are required")
	}
	if len(resourceIDs) == 0 {
		return nil, fmt.Errorf("resourceIDs is required")
	}
	for _, r := range resourceIDs {
		if r == "" {
			return nil, fmt.Errorf("resourceID is required")
		}
	}

	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}

	rollback := func(cause error) (*LockHandle, error) {
		_ = tx.Rollback()
		return nil, cause
	}

	// Shared locks on ancestors.
	if err := lockRow(ctx, tx, userTarget(userID), false); err != nil {
		return rollback(err)
	}
	if err := lockRow(ctx, tx, accountTarget(userID, accountID), false); err != nil {
		return rollback(err)
	}

	// Exclusive locks on resources in deterministic order.
	ordered := append([]string{}, resourceIDs...)
	sort.Strings(ordered)
	for _, r := range ordered {
		if err := lockRow(ctx, tx, resourceTarget(userID, accountID, r), true); err != nil {
			return rollback(err)
		}
	}

	return &LockHandle{tx: tx}, nil
}

type lockTarget struct {
	level  Level
	bucket int
}

func lockKeys(level Level, userID, accountID, resourceID string) ([]lockTarget, error) {
	switch level {
	case LevelUser:
		if userID == "" {
			return nil, fmt.Errorf("userID is required")
		}
		return []lockTarget{userTarget(userID)}, nil
	case LevelAccount:
		if userID == "" || accountID == "" {
			return nil, fmt.Errorf("userID and accountID are required")
		}
		return []lockTarget{userTarget(userID), accountTarget(userID, accountID)}, nil
	case LevelResource:
		if userID == "" || accountID == "" || resourceID == "" {
			return nil, fmt.Errorf("userID, accountID, and resourceID are required")
		}
		return []lockTarget{userTarget(userID), accountTarget(userID, accountID), resourceTarget(userID, accountID, resourceID)}, nil
	default:
		return nil, fmt.Errorf("unknown level")
	}
}

func userTarget(userID string) lockTarget {
	return lockTarget{level: LevelUser, bucket: bucket("user:", userID)}
}

func accountTarget(userID, accountID string) lockTarget {
	return lockTarget{level: LevelAccount, bucket: bucket("account:", userID+":"+accountID)}
}

func resourceTarget(userID, accountID, resourceID string) lockTarget {
	return lockTarget{level: LevelResource, bucket: bucket("resource:", userID+":"+accountID+":"+resourceID)}
}

func bucket(prefix, s string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(prefix))
	_, _ = h.Write([]byte(s))
	return int(h.Sum32() % lockBucketSpace)
}

func lockRow(ctx context.Context, tx *sql.Tx, target lockTarget, exclusive bool) error {
	// NOTE:
	// - We intentionally DO NOT use NOWAIT here: callers/tests can observe real
	//   blocking behavior.
	// - The row must exist (bucket rows are expected to be pre-provisioned).
	var query string
	if exclusive {
		query = "SELECT bucket FROM hier_lock_buckets WHERE level = ? AND bucket = ? FOR UPDATE"
	} else {
		query = "SELECT bucket FROM hier_lock_buckets WHERE level = ? AND bucket = ? FOR SHARE"
	}

	var got int
	if err := tx.QueryRowContext(ctx, query, int(target.level), target.bucket).Scan(&got); err != nil {
		return fmt.Errorf("lock level=%d bucket=%d (exclusive=%v): %w", target.level, target.bucket, exclusive, err)
	}
	return nil
}
