package domain

import (
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestNewSnapshot(t *testing.T) {
	now := time.Date(2026, 10, 8, 18, 4, 0, 0, time.FixedZone("CEST", 2*3600))
	snap, err := NewSnapshot("snap-1", "tenant-a", "vol-1", "éjszakai mentés", now)
	if err != nil {
		t.Fatal(err)
	}
	if snap.ID != "snap-1" || snap.TenantID != "tenant-a" || snap.VolumeID != "vol-1" || snap.Name != "éjszakai mentés" {
		t.Fatalf("identity = %+v", snap)
	}
	if snap.Status != StatusPending || snap.Attempts != 0 || snap.Error != "" {
		t.Fatalf("initial state = %+v", snap)
	}
	if snap.CreatedAt != now.UTC() || snap.UpdatedAt != now.UTC() {
		t.Fatalf("timestamps = %s %s", snap.CreatedAt, snap.UpdatedAt)
	}
	if snap.CreatedAt.Location() != time.UTC || snap.UpdatedAt.Location() != time.UTC {
		t.Fatal("timestamps should be stored in UTC")
	}
}

func TestNewSnapshotRejectsInvalidFields(t *testing.T) {
	now := time.Date(2026, 10, 8, 18, 4, 0, 0, time.UTC)
	tests := []struct {
		name     string
		id       string
		tenantID string
		volumeID string
		snapName string
	}{
		{name: "empty id", tenantID: "tenant-a", volumeID: "vol-1", snapName: "nightly"},
		{name: "id with space", id: "snap 1", tenantID: "tenant-a", volumeID: "vol-1", snapName: "nightly"},
		{name: "empty tenant", id: "snap-1", volumeID: "vol-1", snapName: "nightly"},
		{name: "empty volume", id: "snap-1", tenantID: "tenant-a", snapName: "nightly"},
		{name: "empty name", id: "snap-1", tenantID: "tenant-a", volumeID: "vol-1"},
		{name: "name with newline", id: "snap-1", tenantID: "tenant-a", volumeID: "vol-1", snapName: "bad\nname"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewSnapshot(tc.id, tc.tenantID, tc.volumeID, tc.snapName, now)
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestValidateName(t *testing.T) {
	if err := ValidateName(strings.Repeat("á", MaxNameLen)); err != nil {
		t.Fatal(err)
	}
	if err := ValidateName(strings.Repeat("a", MaxNameLen+1)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("long name: %v", err)
	}
	if err := ValidateName("bad\nname"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("control character: %v", err)
	}
}

func TestValidateScopeID(t *testing.T) {
	if err := ValidateScopeID("tenant_id", ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty: %v", err)
	}
	if err := ValidateScopeID("tenant_id", "has space"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("space: %v", err)
	}
	if err := ValidateScopeID("volume_id", "has\tbreak"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("tab: %v", err)
	}
	if err := ValidateScopeID("volume_id", strings.Repeat("v", MaxIDLen)); err != nil {
		t.Fatal(err)
	}
	if err := ValidateScopeID("volume_id", strings.Repeat("v", MaxIDLen+1)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("long id: %v", err)
	}
}

func TestValidateIdempotencyKey(t *testing.T) {
	if err := ValidateIdempotencyKey(""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty: %v", err)
	}
	if err := ValidateIdempotencyKey("key 1"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("space: %v", err)
	}
	if err := ValidateIdempotencyKey(strings.Repeat("k", MaxIdempotencyKeyLen)); err != nil {
		t.Fatal(err)
	}
	if err := ValidateIdempotencyKey(strings.Repeat("k", MaxIdempotencyKeyLen+1)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("long key: %v", err)
	}
}

func TestNewID(t *testing.T) {
	pattern := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	first, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("ids collided: %s", first)
	}
	for _, id := range []string{first, second} {
		if !pattern.MatchString(id) {
			t.Errorf("id = %q", id)
		}
	}
}
