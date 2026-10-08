package domain

import (
	"errors"
	"fmt"
	"time"
)

// ErrInvalidTransition marks a status change that is not on the graph.
var ErrInvalidTransition = errors.New("invalid snapshot transition")

// ErrNilSnapshot is returned when a transition is applied to a nil snapshot.
var ErrNilSnapshot = errors.New("nil snapshot")

// TransitionError names the edge that was rejected.
type TransitionError struct {
	From Status
	To   Status
}

func (e *TransitionError) Error() string {
	if e == nil {
		return ErrInvalidTransition.Error()
	}
	return fmt.Sprintf("invalid snapshot transition from %s to %s", e.From, e.To)
}

func (e *TransitionError) Unwrap() error {
	return ErrInvalidTransition
}

// CanTransition reports whether from may move directly to to.
//
// The legal edges are:
//
//	pending        -> creating
//	creating       -> ready | failed
//	ready          -> deleting
//	failed         -> deleting
//	error_deleting -> deleting
//	deleting       -> deleted | error_deleting
//
// deleted is terminal. A snapshot cannot skip a step, and it cannot stay
// in the same status by transitioning to itself.
func CanTransition(from, to Status) bool {
	switch from {
	case StatusPending:
		return to == StatusCreating
	case StatusCreating:
		return to == StatusReady || to == StatusFailed
	case StatusReady, StatusFailed, StatusErrorDeleting:
		return to == StatusDeleting
	case StatusDeleting:
		return to == StatusDeleted || to == StatusErrorDeleting
	default:
		return false
	}
}

// Deletable reports whether a delete may start from s.
// That is ready, failed, and error_deleting: the statuses with an edge to deleting.
// pending, creating, deleting, and deleted cannot be deleted.
func (s Status) Deletable() bool {
	return CanTransition(s, StatusDeleting)
}

// CountsTowardQuota reports whether a snapshot in s occupies a tenant slot.
// deleted is the only status that does not. An unrecognized status still counts,
// so a corrupt value cannot slip past the quota.
func (s Status) CountsTowardQuota() bool {
	return s != StatusDeleted
}

// Transition moves s to next and stamps UpdatedAt when the edge is legal.
// Attempts and Error are left unchanged. A rejected edge leaves s as it was.
func Transition(s *Snapshot, next Status, now time.Time) error {
	if s == nil {
		return ErrNilSnapshot
	}
	if !CanTransition(s.Status, next) {
		return &TransitionError{From: s.Status, To: next}
	}
	s.Status = next
	s.UpdatedAt = now
	return nil
}
