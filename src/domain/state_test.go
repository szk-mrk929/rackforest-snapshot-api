package domain

import (
	"errors"
	"testing"
	"time"
)

func TestStatuses(t *testing.T) {
	want := []Status{
		"pending",
		"creating",
		"ready",
		"failed",
		"deleting",
		"error_deleting",
		"deleted",
	}
	got := Statuses()
	if len(got) != len(want) {
		t.Fatalf("Statuses() len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Statuses()[%d] = %q, want %q", i, got[i], want[i])
		}
		if !got[i].Valid() {
			t.Errorf("Valid(%q) = false", got[i])
		}
	}
	for _, s := range []Status{"", "done", "PENDING", "error-deleting"} {
		if s.Valid() {
			t.Errorf("Valid(%q) = true", s)
		}
	}
}

func TestCanTransition(t *testing.T) {
	legal := map[[2]Status]bool{
		{StatusPending, StatusCreating}:       true,
		{StatusCreating, StatusReady}:         true,
		{StatusCreating, StatusFailed}:        true,
		{StatusReady, StatusDeleting}:         true,
		{StatusFailed, StatusDeleting}:        true,
		{StatusErrorDeleting, StatusDeleting}: true,
		{StatusDeleting, StatusDeleted}:       true,
		{StatusDeleting, StatusErrorDeleting}: true,
	}
	if len(legal) != 8 {
		t.Fatalf("legal edges = %d, want 8", len(legal))
	}

	subjects := append(Statuses(), "", "done")
	seen := 0
	for _, from := range subjects {
		for _, to := range subjects {
			want := legal[[2]Status{from, to}]
			if want {
				seen++
			}
			if got := CanTransition(from, to); got != want {
				t.Errorf("CanTransition(%q, %q) = %v, want %v", from, to, got, want)
			}
		}
	}
	if seen != len(legal) {
		t.Fatalf("matrix hit %d legal edges, want %d", seen, len(legal))
	}
}

func TestTransitionAgreesWithGraph(t *testing.T) {
	created := time.Date(2026, 10, 8, 18, 4, 0, 0, time.UTC)
	later := created.Add(time.Second)
	subjects := append(Statuses(), "done")

	for _, from := range subjects {
		for _, to := range subjects {
			snap := Snapshot{
				ID:        "snap-1",
				TenantID:  "tenant-a",
				VolumeID:  "vol-1",
				Name:      "nightly",
				Status:    from,
				Attempts:  2,
				Error:     "previous",
				CreatedAt: created,
				UpdatedAt: created,
			}
			before := snap
			err := Transition(&snap, to, later)
			if !CanTransition(from, to) {
				var edge *TransitionError
				if !errors.As(err, &edge) || !errors.Is(err, ErrInvalidState) {
					t.Errorf("%q -> %q: err = %v", from, to, err)
					continue
				}
				if edge.From != from || edge.To != to {
					t.Errorf("error edge = %q -> %q", edge.From, edge.To)
				}
				if snap != before {
					t.Errorf("%q -> %q changed the snapshot", from, to)
				}
				continue
			}
			if err != nil {
				t.Errorf("%q -> %q: %v", from, to, err)
				continue
			}
			if snap.Status != to || snap.UpdatedAt != later.UTC() || snap.UpdatedAt.Location() != time.UTC {
				t.Errorf("%q -> %q: status %q updated %s", from, to, snap.Status, snap.UpdatedAt)
			}
			if snap.ID != before.ID || snap.Attempts != before.Attempts || snap.Error != before.Error || snap.CreatedAt != before.CreatedAt {
				t.Errorf("%q -> %q changed fields other than status and updated_at: %+v", from, to, snap)
			}
		}
	}
}

func TestTransitionStoresUTC(t *testing.T) {
	created := time.Date(2026, 10, 8, 18, 4, 0, 0, time.FixedZone("CEST", 2*3600))
	snap := Snapshot{Status: StatusPending, CreatedAt: created.UTC(), UpdatedAt: created.UTC()}
	next := created.Add(time.Minute)
	if err := Transition(&snap, StatusCreating, next); err != nil {
		t.Fatal(err)
	}
	if snap.UpdatedAt != next.UTC() || snap.UpdatedAt.Location() != time.UTC {
		t.Fatalf("updated_at = %s", snap.UpdatedAt)
	}
}

func TestLifecycles(t *testing.T) {
	now := time.Date(2026, 10, 8, 18, 4, 0, 0, time.UTC)
	paths := [][]Status{
		{StatusPending, StatusCreating, StatusReady, StatusDeleting, StatusDeleted},
		{StatusPending, StatusCreating, StatusFailed, StatusDeleting, StatusErrorDeleting, StatusDeleting, StatusDeleted},
	}
	for _, path := range paths {
		snap, err := NewSnapshot("snap-1", "tenant-a", "vol-1", "nightly", now)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Status != path[0] {
			t.Fatalf("start = %q, want %q", snap.Status, path[0])
		}
		for i := 1; i < len(path); i++ {
			step := now.Add(time.Duration(i) * time.Second)
			if err := Transition(&snap, path[i], step); err != nil {
				t.Fatalf("%s -> %s: %v", path[i-1], path[i], err)
			}
			if snap.Status != path[i] || snap.UpdatedAt != step.UTC() || snap.Attempts != 0 {
				t.Fatalf("after %s -> %s: %+v", path[i-1], path[i], snap)
			}
		}
		if snap.Status.Deletable() {
			t.Fatalf("status %q is deletable at the end of the path", snap.Status)
		}
	}
}

func TestDeletable(t *testing.T) {
	want := map[Status]bool{
		StatusReady:         true,
		StatusFailed:        true,
		StatusErrorDeleting: true,
		StatusPending:       false,
		StatusCreating:      false,
		StatusDeleting:      false,
		StatusDeleted:       false,
		"":                  false,
		"done":              false,
	}
	for status, ok := range want {
		if got := status.Deletable(); got != ok {
			t.Errorf("Deletable(%q) = %v, want %v", status, got, ok)
		}
	}
}

func TestCountsTowardQuota(t *testing.T) {
	if StatusDeleted.CountsTowardQuota() {
		t.Fatal("deleted snapshot counts toward quota")
	}
	for _, s := range []Status{
		StatusPending,
		StatusCreating,
		StatusReady,
		StatusFailed,
		StatusDeleting,
		StatusErrorDeleting,
		"",
		"done",
	} {
		if !s.CountsTowardQuota() {
			t.Errorf("%q does not count toward quota", s)
		}
	}
}
