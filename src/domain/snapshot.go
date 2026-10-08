// Package domain holds the snapshot model and the rules of its lifecycle.
// It does not know about HTTP or storage, so the state machine and the value
// checks can be tested without either.
package domain

import (
	"crypto/rand"
	"fmt"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	// MaxNameLen is the longest accepted snapshot name, in characters.
	MaxNameLen = 128
	// MaxIDLen is the longest accepted tenant, volume, or snapshot id, in characters.
	MaxIDLen = 128
	// MaxIdempotencyKeyLen is the longest accepted Idempotency-Key value, in characters.
	MaxIdempotencyKeyLen = 255
)

// Snapshot is one point-in-time copy of a volume, as the store sees it.
// Attempts and Error belong to the operation in progress; Transition does not change them.
type Snapshot struct {
	ID        string
	TenantID  string
	VolumeID  string
	Name      string
	Status    Status
	Attempts  int
	Error     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewSnapshot returns a pending snapshot.
// id is supplied by the caller, usually from NewID, and must already be unique.
// Timestamps are stored in UTC.
func NewSnapshot(id, tenantID, volumeID, name string, now time.Time) (Snapshot, error) {
	if err := ValidateScopeID("snapshot_id", id); err != nil {
		return Snapshot{}, err
	}
	if err := ValidateScopeID("tenant_id", tenantID); err != nil {
		return Snapshot{}, err
	}
	if err := ValidateScopeID("volume_id", volumeID); err != nil {
		return Snapshot{}, err
	}
	if err := ValidateName(name); err != nil {
		return Snapshot{}, err
	}
	now = now.UTC()
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

// ValidateName rejects an empty name, a name past MaxNameLen, and any control character.
// Spaces and non-ASCII letters are accepted.
func ValidateName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: name is required", ErrInvalidArgument)
	}
	if utf8.RuneCountInString(name) > MaxNameLen {
		return fmt.Errorf("%w: name must be at most %d characters", ErrInvalidArgument, MaxNameLen)
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return fmt.Errorf("%w: name contains a control character", ErrInvalidArgument)
		}
	}
	return nil
}

// ValidateScopeID checks a tenant, volume, or snapshot identifier.
// Whitespace is rejected so the value round-trips through a header or a URL path.
func ValidateScopeID(kind, value string) error {
	if value == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalidArgument, kind)
	}
	if utf8.RuneCountInString(value) > MaxIDLen {
		return fmt.Errorf("%w: %s must be at most %d characters", ErrInvalidArgument, kind, MaxIDLen)
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return fmt.Errorf("%w: %s contains whitespace or a control character", ErrInvalidArgument, kind)
		}
	}
	return nil
}

// ValidateIdempotencyKey checks a key the caller actually sent.
// An absent key is not an error: the caller skips this check and treats the request as new.
func ValidateIdempotencyKey(key string) error {
	if key == "" {
		return fmt.Errorf("%w: idempotency key is empty", ErrInvalidArgument)
	}
	if utf8.RuneCountInString(key) > MaxIdempotencyKeyLen {
		return fmt.Errorf("%w: idempotency key must be at most %d characters", ErrInvalidArgument, MaxIdempotencyKeyLen)
	}
	for _, r := range key {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return fmt.Errorf("%w: idempotency key contains whitespace or a control character", ErrInvalidArgument)
		}
	}
	return nil
}

// NewID returns a random UUID version 4.
// The service uses it as the snapshot id and passes the same value to storage.
func NewID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
