package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"rackforest-snapshot-api/src/backend"
	"rackforest-snapshot-api/src/domain"
	"rackforest-snapshot-api/src/metrics"
	"rackforest-snapshot-api/src/store"
)

func TestRecoverResumesPendingCreatingAndDeleting(t *testing.T) {
	st := store.NewMemory()
	be := backend.NewScripted()
	rec := metrics.New()
	w := newTestWorker(t, st, be, nil, Config{
		WorkerCount:     2,
		WorkerAttempts:  3,
		WorkerQueueSize: 1,
		StorageTimeout:  time.Second,
		RetryBaseDelay:  time.Millisecond,
	})
	w.SetRecorder(rec)

	createPending(t, st, "tenant-a", "snap-pending", "vol-1", "pending")
	createPending(t, st, "tenant-a", "snap-creating", "vol-2", "creating")
	if err := walk(t, st, "tenant-a", "snap-creating", domain.StatusCreating); err != nil {
		t.Fatal(err)
	}
	createPending(t, st, "tenant-a", "snap-deleting", "vol-3", "deleting")
	if err := walk(t, st, "tenant-a", "snap-deleting", domain.StatusCreating, domain.StatusReady, domain.StatusDeleting); err != nil {
		t.Fatal(err)
	}

	if err := w.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, st, "tenant-a", "snap-pending", domain.StatusReady)
	waitStatus(t, st, "tenant-a", "snap-creating", domain.StatusReady)
	waitStatus(t, st, "tenant-a", "snap-deleting", domain.StatusDeleted)

	body := rec.Render()
	if !strings.Contains(body, "snapshot_storage_duration_seconds_count") || !strings.Contains(body, "snapshot_queue_depth") {
		t.Fatalf("metrics =\n%s", body)
	}
	shutdownOK(t, w)
}

func TestRecoverAfterCancelledCallDoesNotFailImmediately(t *testing.T) {
	st := store.NewMemory()
	be := backend.NewScripted()
	gate := make(chan struct{})
	be.SetGate(gate)
	w := newTestWorker(t, st, be, nil, Config{
		WorkerCount:     1,
		WorkerAttempts:  3,
		WorkerQueueSize: 4,
		StorageTimeout:  time.Second,
		RetryBaseDelay:  time.Millisecond,
	})
	createPending(t, st, "tenant-a", "snap-1", "vol-1", "nightly")
	if err := w.Enqueue(context.Background(), Job{TenantID: "tenant-a", SnapshotID: "snap-1", VolumeID: "vol-1", Op: OpCreate}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, st, "tenant-a", "snap-1", domain.StatusCreating)
	waitInflightAtLeast(t, be, 1)

	stopCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := w.Shutdown(stopCtx); err == nil {
		t.Fatal("expected shutdown deadline")
	}
	if got := getSnapshot(t, st, "tenant-a", "snap-1"); got.Status != domain.StatusCreating {
		t.Fatalf("after cancel = %s", got.Status)
	}
	close(gate)

	next := newTestWorker(t, st, be, nil, Config{
		WorkerCount:     1,
		WorkerAttempts:  3,
		WorkerQueueSize: 4,
		StorageTimeout:  time.Second,
		RetryBaseDelay:  time.Millisecond,
	})
	if err := next.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	ready := waitStatus(t, st, "tenant-a", "snap-1", domain.StatusReady)
	if ready.Status != domain.StatusReady {
		t.Fatalf("recovered = %+v", ready)
	}
	shutdownOK(t, next)
}

func walk(t *testing.T, st store.Store, tenant, id string, steps ...domain.Status) error {
	t.Helper()
	now := time.Date(2026, 10, 8, 21, 0, 0, 0, time.UTC)
	for i, next := range steps {
		_, err := st.Update(context.Background(), tenant, id, func(s domain.Snapshot) (domain.Snapshot, error) {
			return s, domain.Transition(&s, next, now.Add(time.Duration(i+1)*time.Second))
		})
		if err != nil {
			return err
		}
	}
	return nil
}
