// Package store is the snapshot repository.
// The in-memory implementation is the one this process runs; a database can
// replace it later without the worker or the HTTP layer knowing.
package store

import (
	"context"
	"errors"

	"rackforest-snapshot-api/src/domain"
)

var (
	// ErrNotFound means this tenant has no snapshot with that id.
	// A snapshot owned by another tenant is also not found, so its existence does not leak.
	ErrNotFound = errors.New("snapshot not found")

	// ErrQuotaExceeded means the tenant already holds the maximum number of
	// snapshots that are not deleted.
	ErrQuotaExceeded = errors.New("quota exceeded")

	// ErrIdempotencyConflict means the idempotency key is already stored for
	// this tenant with a different volume or name.
	ErrIdempotencyConflict = errors.New("idempotency conflict")

	// ErrDuplicate means a snapshot with that id is already stored.
	ErrDuplicate = errors.New("duplicate snapshot")

	// ErrInvalidSnapshot means the value cannot be inserted.
	ErrInvalidSnapshot = errors.New("invalid snapshot")
)

// Filter narrows one tenant's list. An empty field does not filter.
type Filter struct {
	VolumeID string
	Status   domain.Status
}

// Store is the persistence boundary used by the API and the worker.
type Store interface {
	// Create inserts snap when the tenant is under quota.
	// If idempotencyKey is already stored for the tenant with the same volume
	// and name, Create returns that snapshot and created is false, including
	// when the tenant is over quota. A different volume or name is a conflict.
	// The quota check and the insert share one lock.
	Create(ctx context.Context, snap domain.Snapshot, quota int, idempotencyKey string) (saved domain.Snapshot, created bool, err error)

	Get(ctx context.Context, tenantID, id string) (domain.Snapshot, error)
	List(ctx context.Context, tenantID string, f Filter) ([]domain.Snapshot, error)

	// Update applies fn while the store is locked.
	// fn must not keep the pointer. A status change is stored only when the
	// domain allows that edge; otherwise the record is left unchanged.
	Update(ctx context.Context, tenantID, id string, fn func(*domain.Snapshot) error) (domain.Snapshot, error)
}
