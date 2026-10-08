package domain

import (
	"errors"
	"fmt"
)

// Sentinel errors shared by the store, the worker, and the HTTP layer.
// Callers distinguish them with errors.Is and map them to status codes at the edge.
var (
	// ErrNotFound means the snapshot does not exist in the caller's tenant.
	// A snapshot owned by another tenant is also not found, so existence does not leak.
	ErrNotFound = errors.New("snapshot not found")

	// ErrQuotaExceeded means the tenant already holds the maximum number of
	// snapshots that are not deleted.
	ErrQuotaExceeded = errors.New("tenant snapshot quota exceeded")

	// ErrInvalidState means the requested transition is not legal for the
	// snapshot's current status.
	ErrInvalidState = errors.New("snapshot state does not allow this operation")

	// ErrIdempotencyConflict means the same Idempotency-Key was reused with a
	// different volume or name.
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")

	// ErrInvalidArgument means the caller supplied a value the service refuses
	// before any state change.
	ErrInvalidArgument = errors.New("invalid argument")

	// ErrShuttingDown means the process is draining and will not accept work.
	ErrShuttingDown = errors.New("service is shutting down")

	// ErrQueueFull means the in-process admission queue cannot take another job.
	ErrQueueFull = errors.New("operation queue is full")
)

// TransitionError names the lifecycle edge that was rejected.
// It unwraps to ErrInvalidState.
type TransitionError struct {
	From Status
	To   Status
}

func (e *TransitionError) Error() string {
	if e == nil {
		return ErrInvalidState.Error()
	}
	return fmt.Sprintf("snapshot state does not allow transition from %s to %s", e.From, e.To)
}

func (e *TransitionError) Unwrap() error {
	return ErrInvalidState
}
