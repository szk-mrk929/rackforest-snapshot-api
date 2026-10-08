// Package store is the snapshot repository.
// The in-memory implementation is the one this process runs; a database can
// replace it later without the worker or the HTTP layer knowing.
//
// Failures are the sentinels in the domain package: ErrNotFound,
// ErrQuotaExceeded, ErrIdempotencyConflict, ErrInvalidArgument, and
// ErrInvalidState.
package store

import (
	"context"

	"rackforest-snapshot-api/src/domain"
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
	// when the tenant is over quota. A different volume or name is
	// domain.ErrIdempotencyConflict. The quota check and the insert share one lock.
	Create(ctx context.Context, snap domain.Snapshot, quota int, idempotencyKey string) (saved domain.Snapshot, created bool, err error)

	Get(ctx context.Context, tenantID, id string) (domain.Snapshot, error)
	List(ctx context.Context, tenantID string, f Filter) ([]domain.Snapshot, error)

	// Update applies fn while the store is locked.
	// fn receives a copy and returns the copy to store. A status change is
	// stored only when the domain allows that edge; otherwise the record is
	// left unchanged and the error is domain.ErrInvalidState.
	Update(ctx context.Context, tenantID, id string, fn func(domain.Snapshot) (domain.Snapshot, error)) (domain.Snapshot, error)
}
