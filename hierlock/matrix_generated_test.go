package hierlock

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// This is a more exhaustive, table-driven matrix.
// We generate many (first, second) combinations and validate whether the second
// acquisition blocks or not while the first is held.
func TestHierarchy_ExhaustiveGeneratedMatrix(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	m := NewManager(db)

	u1 := "u1"
	a1 := "a1"
	r1 := "r1"

	// Pick IDs that avoid bucket collisions so that the matrix expectations remain
	// deterministic even under lock striping.
	u2 := pickDifferentUserIDNonColliding(u1, a1, r1)

	a2 := func() string {
		base := "a2"
		for i := 0; ; i++ {
			cand := base
			if i > 0 {
				cand = fmt.Sprintf("%s_%d", base, i)
			}
			need := map[lockTarget]struct{}{}
			for _, u := range []string{u1, u2} {
				need[accountTarget(u, a1)] = struct{}{}
				need[accountTarget(u, cand)] = struct{}{}
			}
			if len(need) == 4 {
				return cand
			}
		}
	}()

	r2 := func() string {
		base := "r2"
		for i := 0; ; i++ {
			cand := base
			if i > 0 {
				cand = fmt.Sprintf("%s_%d", base, i)
			}
			need := map[lockTarget]struct{}{}
			for _, u := range []string{u1, u2} {
				for _, a := range []string{a1, a2} {
					need[resourceTarget(u, a, r1)] = struct{}{}
					need[resourceTarget(u, a, cand)] = struct{}{}
				}
			}
			if len(need) == 8 {
				return cand
			}
		}
	}()

	users := []string{u1, u2}
	accounts := []string{a1, a2}
	resources := []string{r1, r2}

	specs := make([]acquireSpec, 0, len(users)+len(users)*len(accounts)+len(users)*len(accounts)*len(resources))
	for _, u := range users {
		specs = append(specs, acquireSpec{level: LevelUser, userID: u})
	}
	for _, u := range users {
		for _, a := range accounts {
			specs = append(specs, acquireSpec{level: LevelAccount, userID: u, accountID: a})
		}
	}
	for _, u := range users {
		for _, a := range accounts {
			for _, r := range resources {
				specs = append(specs, acquireSpec{level: LevelResource, userID: u, accountID: a, resourceID: r})
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	setupLockTable(ctx, t, db)

	// Pre-provision bucket rows required by all specs.
	need := map[lockTarget]struct{}{}
	for _, s := range specs {
		for _, tgt := range mustTargets(s.level, s.userID, s.accountID, s.resourceID) {
			need[tgt] = struct{}{}
		}
	}
	flat := make([]lockTarget, 0, len(need))
	for tgt := range need {
		flat = append(flat, tgt)
	}
	seedBuckets(ctx, t, db, flat...)

	for _, first := range specs {
		for _, second := range specs {
			first := first
			second := second

			name := fmt.Sprintf("%s THEN %s", specName(first), specName(second))
			wantBlock := specsConflict(first, second)

			t.Run(name, func(t *testing.T) {
				caseCtx, caseCancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer caseCancel()

				h1, err := m.Acquire(caseCtx, first.level, first.userID, first.accountID, first.resourceID)
				if err != nil {
					t.Fatalf("first acquire: %v", err)
				}
				defer h1.Release()

				done := make(chan struct{})
				var h2 *LockHandle
				var err2 error
				go func() {
					h2, err2 = m.Acquire(caseCtx, second.level, second.userID, second.accountID, second.resourceID)
					close(done)
				}()

				if wantBlock {
					// We don't want to sleep too long for thousands of cases; a short window
					// is enough to observe "it didn't finish quickly".
					select {
					case <-done:
						// If it returns immediately, that's wrong.
						if err2 != nil {
							t.Fatalf("expected blocking (then success), but returned early with err: %v", err2)
						}
						_ = h2.Release()
						t.Fatalf("expected blocking, but acquired immediately")
					case <-time.After(25 * time.Millisecond):
						// ok
					}

					_ = h1.Release()
					select {
					case <-done:
						if err2 != nil {
							t.Fatalf("expected acquire after release, got: %v", err2)
						}
						defer h2.Release()
					case <-time.After(5 * time.Second):
						t.Fatalf("second acquire did not finish in time (possible deadlock)")
					}
					return
				}

				select {
				case <-done:
					if err2 != nil {
						t.Fatalf("second acquire: %v", err2)
					}
					defer h2.Release()
				case <-time.After(350 * time.Millisecond):
					t.Fatalf("expected non-blocking acquire, but it appears blocked")
				}
			})
		}
	}
}

func specName(s acquireSpec) string {
	switch s.level {
	case LevelUser:
		return fmt.Sprintf("User(%s)", s.userID)
	case LevelAccount:
		return fmt.Sprintf("Account(%s/%s)", s.userID, s.accountID)
	case LevelResource:
		return fmt.Sprintf("Resource(%s/%s/%s)", s.userID, s.accountID, s.resourceID)
	default:
		return "Unknown"
	}
}

func specsConflict(a, b acquireSpec) bool {
	la := lockIntent(a)
	lb := lockIntent(b)
	for k, ma := range la {
		if mb, ok := lb[k]; ok {
			// S vs S is compatible; anything involving X conflicts.
			if ma == lockExclusive || mb == lockExclusive {
				return true
			}
		}
	}
	return false
}

type lockMode int

const (
	lockShared lockMode = iota
	lockExclusive
)

func lockIntent(s acquireSpec) map[string]lockMode {
	keys := mustKeys(s.level, s.userID, s.accountID, s.resourceID)
	m := make(map[string]lockMode, len(keys))
	for i, k := range keys {
		if i == len(keys)-1 {
			m[k] = lockExclusive
		} else {
			m[k] = lockShared
		}
	}
	return m
}
