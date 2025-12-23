package hierlock

import (
	"context"
	"testing"
	"time"
)

type acquireSpec struct {
	level      Level
	userID     string
	accountID  string
	resourceID string
}

func TestHierarchy_CompatibilityMatrix(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	m := NewManager(db)

	u1 := "u1"
	a1 := "a1"
	r1 := "r1"
	r2 := pickDifferentResourceID(u1, a1, r1)
	a2 := pickDifferentAccountIDNonCollidingResource(u1, a1, r1)
	u2 := pickDifferentUserIDNonColliding(u1, a1, r1)

	cases := []struct {
		name         string
		first        acquireSpec
		second       acquireSpec
		wantBlock bool
	}{
		{
			name: "resource blocks same resource",
			first: acquireSpec{level: LevelResource, userID: u1, accountID: a1, resourceID: r1},
			second: acquireSpec{level: LevelResource, userID: u1, accountID: a1, resourceID: r1},
			wantBlock: true,
		},
		{
			name: "resource allows different resource same account",
			first: acquireSpec{level: LevelResource, userID: u1, accountID: a1, resourceID: r1},
			second: acquireSpec{level: LevelResource, userID: u1, accountID: a1, resourceID: r2},
			wantBlock: false,
		},
		{
			name: "resource allows different account same user",
			first: acquireSpec{level: LevelResource, userID: u1, accountID: a1, resourceID: r1},
			second: acquireSpec{level: LevelResource, userID: u1, accountID: a2, resourceID: r1},
			wantBlock: false,
		},
		{
			name: "resource allows different user",
			first: acquireSpec{level: LevelResource, userID: u1, accountID: a1, resourceID: r1},
			second: acquireSpec{level: LevelResource, userID: u2, accountID: a1, resourceID: r1},
			wantBlock: false,
		},
		{
			name: "account blocks resource under same account",
			first: acquireSpec{level: LevelAccount, userID: u1, accountID: a1},
			second: acquireSpec{level: LevelResource, userID: u1, accountID: a1, resourceID: r1},
			wantBlock: true,
		},
		{
			name: "resource blocks account on same account",
			first: acquireSpec{level: LevelResource, userID: u1, accountID: a1, resourceID: r1},
			second: acquireSpec{level: LevelAccount, userID: u1, accountID: a1},
			wantBlock: true,
		},
		{
			name: "account blocks same account",
			first: acquireSpec{level: LevelAccount, userID: u1, accountID: a1},
			second: acquireSpec{level: LevelAccount, userID: u1, accountID: a1},
			wantBlock: true,
		},
		{
			name: "account allows different account under same user",
			first: acquireSpec{level: LevelAccount, userID: u1, accountID: a1},
			second: acquireSpec{level: LevelAccount, userID: u1, accountID: a2},
			wantBlock: false,
		},
		{
			name: "user blocks account under same user",
			first: acquireSpec{level: LevelUser, userID: u1},
			second: acquireSpec{level: LevelAccount, userID: u1, accountID: a1},
			wantBlock: true,
		},
		{
			name: "account blocks user (reverse order)",
			first: acquireSpec{level: LevelAccount, userID: u1, accountID: a1},
			second: acquireSpec{level: LevelUser, userID: u1},
			wantBlock: true,
		},
		{
			name: "user blocks resource under same user",
			first: acquireSpec{level: LevelUser, userID: u1},
			second: acquireSpec{level: LevelResource, userID: u1, accountID: a1, resourceID: r1},
			wantBlock: true,
		},
		{
			name: "user allows different user",
			first: acquireSpec{level: LevelUser, userID: u1},
			second: acquireSpec{level: LevelUser, userID: u2},
			wantBlock: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			setupLockTable(ctx, t, db)
			// Pre-provision bucket rows required by both acquisitions.
			need := map[lockTarget]struct{}{}
			for _, spec := range []acquireSpec{tc.first, tc.second} {
				tgts := mustTargets(spec.level, spec.userID, spec.accountID, spec.resourceID)
				for _, tgt := range tgts {
					need[tgt] = struct{}{}
				}
			}
			flat := make([]lockTarget, 0, len(need))
			for tgt := range need {
				flat = append(flat, tgt)
			}
			seedBuckets(ctx, t, db, flat...)

			first, err := m.Acquire(ctx, tc.first.level, tc.first.userID, tc.first.accountID, tc.first.resourceID)
			if err != nil {
				t.Fatalf("first acquire: %v", err)
			}
			defer first.Release()

			done := make(chan struct{})
			var secondHandle *LockHandle
			var secondErr error
			go func() {
				secondHandle, secondErr = m.Acquire(ctx, tc.second.level, tc.second.userID, tc.second.accountID, tc.second.resourceID)
				close(done)
			}()

			if tc.wantBlock {
				select {
				case <-done:
					if secondErr != nil {
						t.Fatalf("expected to block then succeed, but returned early with err: %v", secondErr)
					}
					_ = secondHandle.Release()
					t.Fatalf("expected to block, but acquired immediately")
				case <-time.After(150 * time.Millisecond):
					// ok: still blocked
				}

				_ = first.Release()
				select {
				case <-done:
					if secondErr != nil {
						t.Fatalf("expected acquire after release, got: %v", secondErr)
					}
					defer secondHandle.Release()
				case <-time.After(3 * time.Second):
					t.Fatalf("second acquire did not finish in time (possible deadlock)")
				}
				return
			}

			// Not blocked: should finish quickly even while first is held.
			select {
			case <-done:
				if secondErr != nil {
					t.Fatalf("second acquire: %v", secondErr)
				}
				defer secondHandle.Release()
			case <-time.After(250 * time.Millisecond):
				t.Fatalf("expected non-blocking acquire, but it appears blocked")
			}
		})
	}
}

func TestHierarchy_NoDeadlock_MultiResourceOrdered(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	setupLockTable(ctx, t, db)
	seedBuckets(ctx, t, db,
		userTarget("u1"),
		accountTarget("u1", "a1"),
		resourceTarget("u1", "a1", "r1"),
		resourceTarget("u1", "a1", "r2"),
	)

	m := NewManager(db)

	first, err := m.AcquireResources(ctx, "u1", "a1", []string{"r1", "r2"})
	if err != nil {
		t.Fatalf("first AcquireResources: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		second, err := m.AcquireResources(ctx, "u1", "a1", []string{"r2", "r1"}) // reversed input
		if second != nil {
			defer second.Release()
		}
		done <- err
	}()

	// Second should be blocked while first holds resource locks.
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected second to block, but it completed immediately")
		}
		// If it returned early with deadlock, that's a failure.
		if isDeadlock(err) {
			t.Fatalf("unexpected deadlock: %v", err)
		}
		t.Fatalf("unexpected early error: %v", err)
	case <-time.After(150 * time.Millisecond):
		// ok
	}

	_ = first.Release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second AcquireResources should succeed after release, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("second AcquireResources did not finish (possible deadlock)")
	}
}

