package hierlock

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestHierarchy_ResourceBlocksAccountExclusive(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	setupLockTable(ctx, t, db)

	// user1/accountA/resourceX
	keys := append([]string{}, mustKeys(LevelResource, "user1", "accountA", "resourceX")...)
	keys = append(keys, mustKeys(LevelAccount, "user1", "accountA", "")...)
	seedLockKeys(ctx, t, db, keys...)

	m := NewManager(db)

	lock1, err := m.Acquire(ctx, LevelResource, "user1", "accountA", "resourceX")
	if err != nil {
		t.Fatalf("acquire resource lock: %v", err)
	}
	defer lock1.Release()

	// Should conflict because account row is held FOR SHARE by resource lock.
	_, err = m.Acquire(ctx, LevelAccount, "user1", "accountA", "")
	if err == nil {
		t.Fatalf("expected conflict, got nil")
	}
	if !isLockConflict(err) {
		t.Fatalf("expected lock conflict mysql error, got: %v", err)
	}
}

func TestHierarchy_ResourceBlocksUserExclusive(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	setupLockTable(ctx, t, db)
	keys := append([]string{}, mustKeys(LevelResource, "user1", "accountA", "resourceX")...)
	keys = append(keys, mustKeys(LevelUser, "user1", "", "")...)
	seedLockKeys(ctx, t, db, keys...)

	m := NewManager(db)

	lock1, err := m.Acquire(ctx, LevelResource, "user1", "accountA", "resourceX")
	if err != nil {
		t.Fatalf("acquire resource lock: %v", err)
	}
	defer lock1.Release()

	// Should conflict because user row is held FOR SHARE by resource lock.
	_, err = m.Acquire(ctx, LevelUser, "user1", "", "")
	if err == nil {
		t.Fatalf("expected conflict, got nil")
	}
	if !isLockConflict(err) {
		t.Fatalf("expected lock conflict mysql error, got: %v", err)
	}
}

func TestHierarchy_DifferentResourcesDoNotConflict(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	setupLockTable(ctx, t, db)
	keys := append([]string{}, mustKeys(LevelResource, "user1", "accountA", "resourceX")...)
	keys = append(keys, mustKeys(LevelResource, "user1", "accountA", "resourceY")...)
	seedLockKeys(ctx, t, db, keys...)

	m := NewManager(db)

	lock1, err := m.Acquire(ctx, LevelResource, "user1", "accountA", "resourceX")
	if err != nil {
		t.Fatalf("acquire resourceX: %v", err)
	}
	defer lock1.Release()

	// Should succeed: user/account are shared-compatible, resource row differs.
	lock2, err := m.Acquire(ctx, LevelResource, "user1", "accountA", "resourceY")
	if err != nil {
		t.Fatalf("acquire resourceY: %v", err)
	}
	defer lock2.Release()
}

func TestHierarchy_ConcurrentDifferentResources(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	setupLockTable(ctx, t, db)
	keys := append([]string{}, mustKeys(LevelResource, "user1", "accountA", "resourceX")...)
	keys = append(keys, mustKeys(LevelResource, "user1", "accountA", "resourceY")...)
	seedLockKeys(ctx, t, db, keys...)

	m := NewManager(db)

	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		lock, err := m.Acquire(ctx, LevelResource, "user1", "accountA", "resourceX")
		if err != nil {
			errCh <- err
			return
		}
		defer lock.Release()
	}()

	go func() {
		defer wg.Done()
		lock, err := m.Acquire(ctx, LevelResource, "user1", "accountA", "resourceY")
		if err != nil {
			errCh <- err
			return
		}
		defer lock.Release()
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}
