package domain

import "time"

// Status is one of the seven lifecycle states of a snapshot.
//
//	pending → creating → ready → deleting → deleted
//	                 ↘ failed ↗        ↘ error_deleting
//	                                    (delete may be retried)
type Status string

const (
	// StatusPending means the create was accepted and storage has not started.
	StatusPending Status = "pending"
	// StatusCreating means a create is in progress against storage.
	StatusCreating Status = "creating"
	// StatusReady means the storage snapshot exists.
	StatusReady Status = "ready"
	// StatusFailed means create exhausted its attempts.
	StatusFailed Status = "failed"
	// StatusDeleting means a delete is in progress against storage.
	StatusDeleting Status = "deleting"
	// StatusErrorDeleting means delete exhausted its attempts and may be retried.
	StatusErrorDeleting Status = "error_deleting"
	// StatusDeleted means the storage snapshot is gone and the quota slot is free.
	StatusDeleted Status = "deleted"
)

// statuses is the lifecycle order. Valid and Statuses both read it.
var statuses = [...]Status{
	StatusPending,
	StatusCreating,
	StatusReady,
	StatusFailed,
	StatusDeleting,
	StatusErrorDeleting,
	StatusDeleted,
}

// Statuses returns the seven statuses in lifecycle order.
func Statuses() []Status {
	out := make([]Status, len(statuses))
	copy(out, statuses[:])
	return out
}

// Valid reports whether s is one of the seven statuses.
func (s Status) Valid() bool {
	for _, status := range statuses {
		if s == status {
			return true
		}
	}
	return false
}

// CountsTowardQuota reports whether a snapshot in s occupies a tenant slot.
// deleted is the only status that does not. An unrecognized status still counts,
// so a corrupt value cannot slip past the quota.
func (s Status) CountsTowardQuota() bool {
	return s != StatusDeleted
}

// Deletable reports whether a delete may start from s.
// pending and creating are refused so a delete cannot race the create.
// deleting and deleted have no edge back to deleting either.
func (s Status) Deletable() bool {
	return CanTransition(s, StatusDeleting)
}

// CanTransition reports whether from may move directly to to.
// There is no edge from a status to itself, and deleted is terminal.
func CanTransition(from, to Status) bool {
	switch to {
	case StatusCreating:
		return from == StatusPending
	case StatusReady, StatusFailed:
		return from == StatusCreating
	case StatusDeleting:
		return from == StatusReady || from == StatusFailed || from == StatusErrorDeleting
	case StatusDeleted, StatusErrorDeleting:
		return from == StatusDeleting
	default:
		return false
	}
}

// Transition moves s to next and stamps UpdatedAt in UTC when the edge is legal.
// Attempts and Error stay as they are. A rejected edge leaves s unchanged.
func Transition(s *Snapshot, next Status, now time.Time) error {
	if !CanTransition(s.Status, next) {
		return &TransitionError{From: s.Status, To: next}
	}
	s.Status = next
	s.UpdatedAt = now.UTC()
	return nil
}
