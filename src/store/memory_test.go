package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"rackforest-snapshot-api/src/domain"
)

func TestTenantsDoNotSeeEachOther(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	saved, created, err := m.Create(ctx, pending("a", "tenant-a", "vol-1", "nightly"), 10, "")
	if err != nil || !created || saved.ID != "a" {
		t.Fatalf("create = %+v created=%v err=%v", saved, created, err)
	}

	if _, err := m.Get(ctx, "tenant-b", "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other tenant get = %v", err)
	}
	if _, err := m.Update(ctx, "tenant-b", "a", func(s *domain.Snapshot) error {
		s.Name = "stolen"
		return nil
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other tenant update = %v", err)
	}
	got, err := m.Get(ctx, "tenant-a", "a")
	if err != nil || got.Name != "nightly" {
		t.Fatalf("owner snapshot changed: %+v %v", got, err)
	}

	list, err := m.List(ctx, "tenant-b", Filter{})
	if err != nil || len(list) != 0 {
		t.Fatalf("other tenant list = %+v %v", list, err)
	}
	list, err = m.List(ctx, "tenant-a", Filter{})
	if err != nil || len(list) != 1 || list[0].ID != "a" {
		t.Fatalf("owner list = %+v %v", list, err)
	}
}

func TestListFiltersVolumeAndStatus(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	for _, snap := range []domain.Snapshot{
		pending("a", "tenant-a", "vol-1", "one"),
		pending("b", "tenant-a", "vol-2", "two"),
		pending("c", "tenant-b", "vol-1", "three"),
	} {
		if _, _, err := m.Create(ctx, snap, 10, ""); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.Update(ctx, "tenant-a", "a", func(s *domain.Snapshot) error {
		return domain.Transition(s, domain.StatusCreating, s.UpdatedAt.Add(time.Second))
	}); err != nil {
		t.Fatal(err)
	}

	byVolume, err := m.List(ctx, "tenant-a", Filter{VolumeID: "vol-1"})
	if err != nil || len(byVolume) != 1 || byVolume[0].ID != "a" {
		t.Fatalf("volume list = %+v %v", byVolume, err)
	}
	pendingOnly, err := m.List(ctx, "tenant-a", Filter{Status: domain.StatusPending})
	if err != nil || len(pendingOnly) != 1 || pendingOnly[0].ID != "b" {
		t.Fatalf("status list = %+v %v", pendingOnly, err)
	}
	both, err := m.List(ctx, "tenant-a", Filter{VolumeID: "vol-2", Status: domain.StatusPending})
	if err != nil || len(both) != 1 || both[0].ID != "b" {
		t.Fatalf("combined list = %+v %v", both, err)
	}
}

func TestEleventhNonDeletedSnapshotDoesNotFit(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		id := string(rune('a' + i))
		if _, _, err := m.Create(ctx, pending(id, "tenant-a", "vol", id), 10, ""); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	if _, _, err := m.Create(ctx, pending("k", "tenant-a", "vol", "k"), 10, ""); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("11th err = %v", err)
	}
	if _, created, err := m.Create(ctx, pending("other", "tenant-b", "vol", "other"), 10, ""); err != nil || !created {
		t.Fatalf("other tenant = created %v err %v", created, err)
	}
}

func TestDeletedDoesNotCountTowardQuota(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	if _, _, err := m.Create(ctx, pending("a", "tenant-a", "vol", "one"), 1, ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Create(ctx, pending("b", "tenant-a", "vol", "two"), 1, ""); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("second err = %v", err)
	}
	if err := walk(m, "tenant-a", "a", domain.StatusCreating, domain.StatusReady, domain.StatusDeleting, domain.StatusDeleted); err != nil {
		t.Fatal(err)
	}
	got, err := m.Get(ctx, "tenant-a", "a")
	if err != nil || got.Status != domain.StatusDeleted {
		t.Fatalf("deleted snapshot = %+v %v", got, err)
	}
	if _, created, err := m.Create(ctx, pending("b", "tenant-a", "vol", "two"), 1, ""); err != nil || !created {
		t.Fatalf("after delete = created %v err %v", created, err)
	}
}

func TestIdempotency(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	first, created, err := m.Create(ctx, pending("a", "tenant-a", "vol-1", "nightly"), 10, "nightly-1")
	if err != nil || !created {
		t.Fatal(err, created)
	}
	if err := walk(m, "tenant-a", "a", domain.StatusCreating); err != nil {
		t.Fatal(err)
	}

	again, created, err := m.Create(ctx, pending("b", "tenant-a", "vol-1", "nightly"), 10, "nightly-1")
	if err != nil || created || again.ID != first.ID || again.Status != domain.StatusCreating {
		t.Fatalf("replay = %+v created=%v err=%v", again, created, err)
	}
	if _, _, err := m.Create(ctx, pending("c", "tenant-a", "vol-1", "other"), 10, "nightly-1"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("name conflict = %v", err)
	}
	if _, _, err := m.Create(ctx, pending("d", "tenant-a", "vol-2", "nightly"), 10, "nightly-1"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("volume conflict = %v", err)
	}
	if _, created, err := m.Create(ctx, pending("e", "tenant-b", "vol-9", "other"), 10, "nightly-1"); err != nil || !created {
		t.Fatalf("other tenant key = created %v err %v", created, err)
	}
	second, created, err := m.Create(ctx, pending("f", "tenant-a", "vol-1", "nightly"), 10, "")
	if err != nil || !created || second.ID == first.ID {
		t.Fatalf("empty key = %+v created=%v err=%v", second, created, err)
	}
}

func TestIdempotentReplayIgnoresQuota(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	first, _, err := m.Create(ctx, pending("a", "tenant-a", "vol", "nightly"), 1, "k")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Create(ctx, pending("b", "tenant-a", "vol", "other"), 1, ""); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("quota err = %v", err)
	}
	again, created, err := m.Create(ctx, pending("c", "tenant-a", "vol", "nightly"), 1, "k")
	if err != nil || created || again.ID != first.ID {
		t.Fatalf("replay over quota = %+v created=%v err=%v", again, created, err)
	}
}

func TestUpdateRejectsIllegalTransition(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	if _, _, err := m.Create(ctx, pending("a", "tenant-a", "vol", "nightly"), 10, ""); err != nil {
		t.Fatal(err)
	}
	_, err := m.Update(ctx, "tenant-a", "a", func(s *domain.Snapshot) error {
		s.Status = domain.StatusReady
		s.Attempts = 3
		s.Error = "skipped"
		return nil
	})
	var edge *domain.TransitionError
	if !errors.As(err, &edge) || !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("err = %v", err)
	}
	if edge.From != domain.StatusPending || edge.To != domain.StatusReady {
		t.Fatalf("edge = %s -> %s", edge.From, edge.To)
	}
	got, err := m.Get(ctx, "tenant-a", "a")
	if err != nil || got.Status != domain.StatusPending || got.Attempts != 0 || got.Error != "" {
		t.Fatalf("record changed: %+v %v", got, err)
	}

	updated, err := m.Update(ctx, "tenant-a", "a", func(s *domain.Snapshot) error {
		s.Attempts = 1
		s.Error = "busy"
		return domain.Transition(s, domain.StatusCreating, s.UpdatedAt.Add(time.Second))
	})
	if err != nil || updated.Status != domain.StatusCreating || updated.Attempts != 1 || updated.Error != "busy" {
		t.Fatalf("legal update = %+v %v", updated, err)
	}
}

func TestUpdateKeepsIdentity(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	createdAt := time.Date(2026, 10, 8, 18, 0, 0, 0, time.UTC)
	snap := pending("a", "tenant-a", "vol-1", "nightly")
	snap.CreatedAt = createdAt
	snap.UpdatedAt = createdAt
	if _, _, err := m.Create(ctx, snap, 10, ""); err != nil {
		t.Fatal(err)
	}
	got, err := m.Update(ctx, "tenant-a", "a", func(s *domain.Snapshot) error {
		s.ID = "other"
		s.TenantID = "tenant-b"
		s.VolumeID = "vol-9"
		s.Name = "renamed"
		s.CreatedAt = createdAt.Add(time.Hour)
		s.Attempts = 2
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "a" || got.TenantID != "tenant-a" || got.VolumeID != "vol-1" || got.Name != "nightly" || !got.CreatedAt.Equal(createdAt) {
		t.Fatalf("identity changed: %+v", got)
	}
	if got.Attempts != 2 {
		t.Fatalf("attempts = %d", got.Attempts)
	}
	if _, err := m.Get(ctx, "tenant-b", "other"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("renamed record is visible: %v", err)
	}
}

func TestQuotaIsCountedWithTheInsert(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	const n = 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	createdN, quotaN := 0, 0
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			id, err := domain.NewID()
			if err != nil {
				t.Error(err)
				return
			}
			_, created, err := m.Create(ctx, pending(id, "tenant-a", "vol", id), 10, "")
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil && created:
				createdN++
			case errors.Is(err, ErrQuotaExceeded):
				quotaN++
			default:
				t.Errorf("create err = %v", err)
			}
		}(i)
	}
	wg.Wait()
	if createdN != 10 || quotaN != n-10 {
		t.Fatalf("created=%d quota=%d", createdN, quotaN)
	}
}

func TestCreateRejectsDuplicateAndBadInput(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	if _, _, err := m.Create(ctx, pending("a", "tenant-a", "vol", "one"), 10, ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Create(ctx, pending("a", "tenant-b", "vol", "two"), 10, ""); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate = %v", err)
	}
	blank := pending("", "tenant-a", "vol", "one")
	if _, _, err := m.Create(ctx, blank, 10, ""); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("blank id = %v", err)
	}
	ready := pending("b", "tenant-a", "vol", "two")
	ready.Status = domain.StatusReady
	if _, _, err := m.Create(ctx, ready, 10, ""); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("ready create = %v", err)
	}
}

func TestCanceledContext(t *testing.T) {
	m := NewMemory()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := m.Create(ctx, pending("a", "tenant-a", "vol", "one"), 10, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("create err = %v", err)
	}
	if _, err := m.Get(ctx, "tenant-a", "a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("get err = %v", err)
	}
}

func pending(id, tenant, volume, name string) domain.Snapshot {
	now := time.Date(2026, 10, 8, 18, 0, 0, 0, time.UTC)
	return domain.Snapshot{
		ID:        id,
		TenantID:  tenant,
		VolumeID:  volume,
		Name:      name,
		Status:    domain.StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func walk(m *Memory, tenant, id string, steps ...domain.Status) error {
	ctx := context.Background()
	for i, status := range steps {
		_, err := m.Update(ctx, tenant, id, func(s *domain.Snapshot) error {
			return domain.Transition(s, status, s.UpdatedAt.Add(time.Duration(i+1)*time.Second))
		})
		if err != nil {
			return err
		}
	}
	return nil
}
