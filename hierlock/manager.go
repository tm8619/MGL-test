package hierlock

import (
	"context"
	"database/sql"
	"fmt"
)

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

func lockKeys(level Level, userID, accountID, resourceID string) ([]string, error) {
	switch level {
	case LevelUser:
		if userID == "" {
			return nil, fmt.Errorf("userID is required")
		}
		return []string{userKey(userID)}, nil
	case LevelAccount:
		if userID == "" || accountID == "" {
			return nil, fmt.Errorf("userID and accountID are required")
		}
		return []string{userKey(userID), accountKey(userID, accountID)}, nil
	case LevelResource:
		if userID == "" || accountID == "" || resourceID == "" {
			return nil, fmt.Errorf("userID, accountID, and resourceID are required")
		}
		return []string{userKey(userID), accountKey(userID, accountID), resourceKey(userID, accountID, resourceID)}, nil
	default:
		return nil, fmt.Errorf("unknown level")
	}
}

func userKey(userID string) string {
	return "user:" + userID
}

func accountKey(userID, accountID string) string {
	return "account:" + userID + ":" + accountID
}

func resourceKey(userID, accountID, resourceID string) string {
	return "resource:" + userID + ":" + accountID + ":" + resourceID
}

func lockRow(ctx context.Context, tx *sql.Tx, lockKey string, exclusive bool) error {
	// NOTE:
	// - MySQL 8.0 supports NOWAIT for FOR UPDATE / FOR SHARE.
	// - We rely on NOWAIT to make tests deterministic (no sleeping).
	// - The row must exist (we pre-create rows in tests).
	var query string
	if exclusive {
		query = "SELECT lock_key FROM hier_locks WHERE lock_key = ? FOR UPDATE NOWAIT"
	} else {
		query = "SELECT lock_key FROM hier_locks WHERE lock_key = ? FOR SHARE NOWAIT"
	}

	var got string
	if err := tx.QueryRowContext(ctx, query, lockKey).Scan(&got); err != nil {
		return fmt.Errorf("lock %q (exclusive=%v): %w", lockKey, exclusive, err)
	}
	return nil
}
