// Package backend is the storage system behind snapshot creation and deletion.
// The random mock stands in for a slow, fallible backend. Tests use the
// scripted implementation so a result does not depend on chance.
package backend

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrCreateFailed is a transient failure from CreateSnapshot.
	ErrCreateFailed = errors.New("storage create failed")

	// ErrDeleteFailed is a transient failure from DeleteSnapshot.
	ErrDeleteFailed = errors.New("storage delete failed")
)

// StorageBackend creates and deletes snapshots on the storage system.
// Both calls are idempotent: creating a snapshot that already exists succeeds,
// and deleting one that was never created succeeds.
type StorageBackend interface {
	CreateSnapshot(ctx context.Context, volumeID, snapshotID string) error
	DeleteSnapshot(ctx context.Context, volumeID, snapshotID string) error
}

func wait(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
