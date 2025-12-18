package hierlock

import (
	"context"
	"database/sql"
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

	cases := []struct {
		name         string
		first        acquireSpec
		second       acquireSpec
		wantConflict bool
	}{
		{
			name: "resource blocks same resource",
			first: acquireSpec{level: LevelResource, userID: "u1", accountID: "a1", resourceID: "r1"},
			second: acquireSpec{level: LevelResource, userID: "u1", accountID: "a1", resourceID: "r1"},
			wantConflict: true,
		},
		{
			name: "resource allows different resource same account",
			first: acquireSpec{level: LevelResource, userID: "u1", accountID: "a1", resourceID: "r1"},
			second: acquireSpec{level: LevelResource, userID: "u1", accountID: "a1", resourceID: "r2"},
			wantConflict: false,
		},
		{
			name: "resource allows different account same user",
			first: acquireSpec{level: LevelResource, userID: "u1", accountID: "a1", resourceID: "r1"},
			second: acquireSpec{level: LevelResource, userID: "u1", accountID: "a2", resourceID: "r1"},
			wantConflict: false,
		},
		{
			name: "resource allows different user",
			first: acquireSpec{level: LevelResource, userID: "u1", accountID: "a1", resourceID: "r1"},
			second: acquireSpec{level: LevelResource, userID: "u2", accountID: "a1", resourceID: "r1"},
			wantConflict: false,
		},
		{
			name: "account blocks resource under same account",
			first: acquireSpec{level: LevelAccount, userID: "u1", accountID: "a1"},
			second: acquireSpec{level: LevelResource, userID: "u1", accountID: "a1", resourceID: "r1"},
			wantConflict: true,
		},
		{
			name: "resource blocks account on same account",
			first: acquireSpec{level: LevelResource, userID: "u1", accountID: "a1", resourceID: "r1"},
			second: acquireSpec{level: LevelAccount, userID: "u1", accountID: "a1"},
			wantConflict: true,
		},
		{
			name: "account blocks same account",
			first: acquireSpec{level: LevelAccount, userID: "u1", accountID: "a1"},
			second: acquireSpec{level: LevelAccount, userID: "u1", accountID: "a1"},
			wantConflict: true,
		},
		{
			name: "account allows different account under same user",
			first: acquireSpec{level: LevelAccount, userID: "u1", accountID: "a1"},
			second: acquireSpec{level: LevelAccount, userID: "u1", accountID: "a2"},
			wantConflict: false,
		},
		{
			name: "user blocks account under same user",
			first: acquireSpec{level: LevelUser, userID: "u1"},
			second: acquireSpec{level: LevelAccount, userID: "u1", accountID: "a1"},
			wantConflict: true,
		},
		{
			name: "account blocks user (reverse order)",
			first: acquireSpec{level: LevelAccount, userID: "u1", accountID: "a1"},
			second: acquireSpec{level: LevelUser, userID: "u1"},
			wantConflict: true,
		},
		{
			name: "user blocks resource under same user",
			first: acquireSpec{level: LevelUser, userID: "u1"},
			second: acquireSpec{level: LevelResource, userID: "u1", accountID: "a1", resourceID: "r1"},
			wantConflict: true,
		},
		{
			name: "user allows different user",
			first: acquireSpec{level: LevelUser, userID: "u1"},
			second: acquireSpec{level: LevelUser, userID: "u2"},
			wantConflict: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			setupLockTable(ctx, t, db)
			seedForTwoAcquires(ctx, t, db, tc.first, tc.second)

			first, err := m.Acquire(ctx, tc.first.level, tc.first.userID, tc.first.accountID, tc.first.resourceID)
			if err != nil {
				t.Fatalf("first acquire: %v", err)
			}
			defer first.Release()

			second, err := m.Acquire(ctx, tc.second.level, tc.second.userID, tc.second.accountID, tc.second.resourceID)
			if tc.wantConflict {
				if err == nil {
					_ = second.Release()
					t.Fatalf("expected conflict, got nil")
				}
				if !isLockConflict(err) {
					t.Fatalf("expected lock conflict mysql error, got: %v", err)
				}
				return
			}

			if err != nil {
				t.Fatalf("second acquire: %v", err)
			}
			defer second.Release()
		})
	}
}

func seedForTwoAcquires(ctx context.Context, t fataler, db *sql.DB, a, b acquireSpec) {
	keys := map[string]struct{}{}
	for _, spec := range []acquireSpec{a, b} {
		for _, k := range mustKeys(spec.level, spec.userID, spec.accountID, spec.resourceID) {
			keys[k] = struct{}{}
		}
	}

	all := make([]string, 0, len(keys))
	for k := range keys {
		all = append(all, k)
	}
	seedLockKeys(ctx, t, db, all...)
}
