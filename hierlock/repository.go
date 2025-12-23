package hierlock

import (
	"context"
	"database/sql"
)

// Repository is a thin wrapper that exposes lock acquisition methods
// aligned with typical application code usage.
//
// It intentionally does not try to hide the LockHandle; callers must Release().
type Repository struct {
	m *Manager
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{m: NewManager(db)}
}

func (r *Repository) GetUserLock(ctx context.Context, userID string) (*LockHandle, error) {
	return r.m.Acquire(ctx, LevelUser, userID, "", "")
}

func (r *Repository) GetAccountLock(ctx context.Context, userID, accountID string) (*LockHandle, error) {
	return r.m.Acquire(ctx, LevelAccount, userID, accountID, "")
}

func (r *Repository) GetResourceLock(ctx context.Context, userID, accountID, resourceID string) (*LockHandle, error) {
	return r.m.Acquire(ctx, LevelResource, userID, accountID, resourceID)
}

func (r *Repository) GetResourcesLock(ctx context.Context, userID, accountID string, resourceIDs []string) (*LockHandle, error) {
	return r.m.AcquireResources(ctx, userID, accountID, resourceIDs)
}
