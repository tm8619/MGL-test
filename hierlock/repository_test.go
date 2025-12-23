package hierlock

import (
	"context"
	"testing"
	"time"
)

func TestRepository_LockMethods(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	setupLockTable(ctx, t, db)
	seedBuckets(ctx, t, db,
		userTarget("u_repo"),
		accountTarget("u_repo", "a_repo"),
		resourceTarget("u_repo", "a_repo", "r_repo"),
		resourceTarget("u_repo", "a_repo", "r_repo_2"),
	)

	repo := NewRepository(db)

	h1, err := repo.GetUserLock(ctx, "u_repo")
	if err != nil {
		t.Fatalf("GetUserLock: %v", err)
	}
	_ = h1.Release()

	h2, err := repo.GetAccountLock(ctx, "u_repo", "a_repo")
	if err != nil {
		t.Fatalf("GetAccountLock: %v", err)
	}
	_ = h2.Release()

	h3, err := repo.GetResourceLock(ctx, "u_repo", "a_repo", "r_repo")
	if err != nil {
		t.Fatalf("GetResourceLock: %v", err)
	}
	_ = h3.Release()

	h4, err := repo.GetResourcesLock(ctx, "u_repo", "a_repo", []string{"r_repo", "r_repo_2"})
	if err != nil {
		t.Fatalf("GetResourcesLock: %v", err)
	}
	_ = h4.Release()
}

func TestBucketKeyRange(t *testing.T) {
	tgt := userTarget("some-user")
	if tgt.level != LevelUser {
		t.Fatalf("unexpected level: %v", tgt.level)
	}
	if tgt.bucket < 0 || tgt.bucket >= lockBucketSpace {
		t.Fatalf("bucket out of range: %d", tgt.bucket)
	}
}
