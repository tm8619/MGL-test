package hierlock

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// This test is a "counter example": if you lock multiple resources in different orders
// across transactions, you can trigger a deadlock in MySQL.
func TestHierarchy_Deadlock_UnorderedMultiResource(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	setupLockTable(ctx, t, db)
	seedLockKeys(ctx, t, db,
		userKey("u1"),
		accountKey("u1", "a1"),
		resourceKey("u1", "a1", "r1"),
		resourceKey("u1", "a1", "r2"),
	)

	ready1 := make(chan struct{})
	ready2 := make(chan struct{})
	startSecond := make(chan struct{})

	resCh := make(chan error, 2)

	go func() {
		err := acquireTwoResourcesUnorderedWithBarrier(ctx, db, "u1", "a1", "r1", "r2", ready1, startSecond)
		resCh <- err
	}()
	go func() {
		err := acquireTwoResourcesUnorderedWithBarrier(ctx, db, "u1", "a1", "r2", "r1", ready2, startSecond)
		resCh <- err
	}()

	// Wait until both transactions have acquired their first resource lock.
	select {
	case <-ready1:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for tx1 to lock first resource")
	}
	select {
	case <-ready2:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for tx2 to lock first resource")
	}

	close(startSecond)

	err1 := <-resCh
	err2 := <-resCh

	if err1 == nil && err2 == nil {
		t.Fatalf("expected a deadlock/timeout from unordered locking, got nil,nil")
	}

	// In an actual deadlock, typically one tx gets ER_LOCK_DEADLOCK (1213) and the other succeeds.
	// Depending on server settings and timing, lock-wait timeout (1205) can also appear.
	if !(isDeadlock(err1) || isDeadlock(err2) || isLockWaitTimeout(err1) || isLockWaitTimeout(err2)) {
		t.Fatalf("expected deadlock(1213) or lock wait timeout(1205), got err1=%v err2=%v", err1, err2)
	}
}

func acquireTwoResourcesUnorderedWithBarrier(
	ctx context.Context,
	db *sql.DB,
	userID, accountID, firstResourceID, secondResourceID string,
	ready chan<- struct{},
	startSecond <-chan struct{},
) (retErr error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// Optional: make lock wait timeout small so we don't hang forever if the server
	// doesn't detect deadlock quickly for some reason.
	_, _ = tx.ExecContext(ctx, "SET SESSION innodb_lock_wait_timeout = 3")

	if err := lockRow(ctx, tx, userKey(userID), false); err != nil {
		return err
	}
	if err := lockRow(ctx, tx, accountKey(userID, accountID), false); err != nil {
		return err
	}

	if err := lockRow(ctx, tx, resourceKey(userID, accountID, firstResourceID), true); err != nil {
		return err
	}

	close(ready)
	<-startSecond

	// This lock attempt should create a deadlock against the other transaction.
	if err := lockRow(ctx, tx, resourceKey(userID, accountID, secondResourceID), true); err != nil {
		return err
	}

	// If we somehow got here, we acquired both locks. Keep them briefly to increase
	// chance the other side is still attempting.
	time.Sleep(150 * time.Millisecond)
	return nil
}
