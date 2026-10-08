// Package domain holds the snapshot model and the rules for moving between
// its seven statuses. Storage and HTTP stay outside this package.
package domain

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Status is one step in the snapshot lifecycle.
type Status string

const (
	StatusPending       Status = "pending"
	StatusCreating      Status = "creating"
	StatusReady         Status = "ready"
	StatusFailed        Status = "failed"
	StatusDeleting      Status = "deleting"
	StatusErrorDeleting Status = "error_deleting"
	StatusDeleted       Status = "deleted"
)

// Snapshot is one tenant's named snapshot of a volume.
// Attempts and Error belong to the operation in progress; this package does
// not change them when the status moves.
type Snapshot struct {
	ID        string    `json:"id"`
	TenantID  string    `json:"tenant_id"`
	VolumeID  string    `json:"volume_id"`
	Name      string    `json:"name"`
	Status    Status    `json:"status"`
	Attempts  int       `json:"attempts"`
	Error     string    `json:"error"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Statuses returns the seven statuses in lifecycle order.
func Statuses() []Status {
	return []Status{
		StatusPending,
		StatusCreating,
		StatusReady,
		StatusFailed,
		StatusDeleting,
		StatusErrorDeleting,
		StatusDeleted,
	}
}

// Valid reports whether s is one of the seven statuses.
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusCreating, StatusReady, StatusFailed,
		StatusDeleting, StatusErrorDeleting, StatusDeleted:
		return true
	default:
		return false
	}
}

// NewID returns a 32-character hex identifier from 16 random bytes.
func NewID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

// New returns a pending snapshot at now.
// The identifiers are stored as given; callers validate them.
func New(tenantID, volumeID, name string, now time.Time) (Snapshot, error) {
	id, err := NewID()
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{
		ID:        id,
		TenantID:  tenantID,
		VolumeID:  volumeID,
		Name:      name,
		Status:    StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}
