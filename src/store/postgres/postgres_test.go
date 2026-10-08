package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"rackforest-snapshot-api/src/domain"
	"rackforest-snapshot-api/src/store"
)

func TestPostgresMatchesTheMemoryContract(t *testing.T) {
	dsn := os.Getenv("SNAPSHOT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("SNAPSHOT_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	st, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)

	if _, err := st.pool.Exec(ctx, `TRUNCATE snapshots`); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 10, 8, 22, 0, 0, 0, time.UTC)
	first := mustSnap(t, "snap-a", "tenant-a", "vol-1", "nightly", now)
	saved, created, err := st.Create(ctx, first, 1, "key-1")
	if err != nil || !created || saved.ID != first.ID {
		t.Fatalf("create = %+v created=%v err=%v", saved, created, err)
	}

	again, created, err := st.Create(ctx, mustSnap(t, "snap-b", "tenant-a", "vol-1", "nightly", now), 1, "key-1")
	if err != nil || created || again.ID != first.ID {
		t.Fatalf("replay = %+v created=%v err=%v", again, created, err)
	}
	if _, _, err := st.Create(ctx, mustSnap(t, "snap-c", "tenant-a", "vol-1", "other", now), 1, "key-1"); !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatalf("conflict = %v", err)
	}
	if _, _, err := st.Create(ctx, mustSnap(t, "snap-d", "tenant-a", "vol-2", "two", now), 1, "key-2"); !errors.Is(err, domain.ErrQuotaExceeded) {
		t.Fatalf("quota = %v", err)
	}

	creating, err := st.Update(ctx, "tenant-a", "snap-a", func(s domain.Snapshot) (domain.Snapshot, error) {
		if err := domain.Transition(&s, domain.StatusCreating, now.Add(time.Second)); err != nil {
			return domain.Snapshot{}, err
		}
		s.Attempts = 1
		return s, nil
	})
	if err != nil || creating.Status != domain.StatusCreating {
		t.Fatalf("update = %+v err=%v", creating, err)
	}
	if _, err := st.Update(ctx, "tenant-a", "snap-a", func(s domain.Snapshot) (domain.Snapshot, error) {
		return s, domain.Transition(&s, domain.StatusDeleted, now)
	}); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("illegal = %v", err)
	}

	ready, err := st.Update(ctx, "tenant-a", "snap-a", func(s domain.Snapshot) (domain.Snapshot, error) {
		return s, domain.Transition(&s, domain.StatusReady, now.Add(2*time.Second))
	})
	if err != nil || ready.Status != domain.StatusReady {
		t.Fatalf("ready = %+v err=%v", ready, err)
	}

	pending := mustSnap(t, "snap-e", "tenant-b", "vol-9", "other", now)
	if _, created, err := st.Create(ctx, pending, 10, "key-1"); err != nil || !created {
		t.Fatalf("other tenant = created %v err %v", created, err)
	}
	got, err := st.Recoverable(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "snap-e" || got[0].Status != domain.StatusPending {
		t.Fatalf("recoverable = %+v", got)
	}
	if err := st.RemovePending(ctx, "tenant-b", "snap-e"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get(ctx, "tenant-b", "snap-e"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("removed = %v", err)
	}
	if _, err := st.Get(ctx, "tenant-b", "snap-a"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("foreign get = %v", err)
	}

	listed, err := st.List(ctx, "tenant-a", store.Filter{})
	if err != nil || len(listed) != 1 || listed[0].ID != "snap-a" {
		t.Fatalf("list = %+v err=%v", listed, err)
	}
}

func mustSnap(t *testing.T, id, tenant, volume, name string, now time.Time) domain.Snapshot {
	t.Helper()
	snap, err := domain.NewSnapshot(id, tenant, volume, name, now)
	if err != nil {
		t.Fatal(err)
	}
	return snap
}
