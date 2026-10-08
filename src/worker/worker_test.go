package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"rackforest-snapshot-api/src/backend"
	"rackforest-snapshot-api/src/domain"
	"rackforest-snapshot-api/src/logger"
	"rackforest-snapshot-api/src/store"
)

func TestCreateAndDeleteSuccess(t *testing.T) {
	st := store.NewMemory()
	be := backend.NewScripted()
	w := newTestWorker(t, st, be, nil, Config{
		WorkerCount:     2,
		WorkerAttempts:  3,
		WorkerQueueSize: 16,
		StorageTimeout:  200 * time.Millisecond,
		RetryBaseDelay:  5 * time.Millisecond,
	})

	createPending(t, st, "tenant-a", "snap-1", "vol-1", "nightly")
	if err := w.Enqueue(context.Background(), Job{
		TenantID:   "tenant-a",
		SnapshotID: "snap-1",
		VolumeID:   "vol-1",
		Op:         OpCreate,
	}); err != nil {
		t.Fatal(err)
	}
	got := waitStatus(t, st, "tenant-a", "snap-1", domain.StatusReady)
	if got.Attempts != 1 || got.Error != "" {
		t.Fatalf("unexpected create result: %+v", got)
	}

	if err := w.Enqueue(context.Background(), Job{
		TenantID:   "tenant-a",
		SnapshotID: "snap-1",
		VolumeID:   "vol-1",
		Op:         OpDelete,
	}); err != nil {
		t.Fatal(err)
	}
	deleted := waitStatus(t, st, "tenant-a", "snap-1", domain.StatusDeleted)
	if deleted.Attempts != 1 || deleted.Error != "" {
		t.Fatalf("unexpected delete result: %+v", deleted)
	}

	shutdownOK(t, w)
}

func TestThreeStorageFailuresEndInFailed(t *testing.T) {
	st := store.NewMemory()
	be := backend.NewScripted()
	be.SetFailCreates(3)
	w := newTestWorker(t, st, be, nil, Config{
		WorkerCount:     1,
		WorkerAttempts:  3,
		WorkerQueueSize: 8,
		StorageTimeout:  200 * time.Millisecond,
		RetryBaseDelay:  time.Millisecond,
	})

	createPending(t, st, "tenant-a", "snap-1", "vol-1", "nightly")
	if err := w.Enqueue(context.Background(), Job{
		TenantID:   "tenant-a",
		SnapshotID: "snap-1",
		VolumeID:   "vol-1",
		Op:         OpCreate,
	}); err != nil {
		t.Fatal(err)
	}

	got := waitStatus(t, st, "tenant-a", "snap-1", domain.StatusFailed)
	if got.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", got.Attempts)
	}
	if got.Error == "" || !strings.Contains(got.Error, backend.ErrCreateFailed.Error()) {
		t.Fatalf("error = %q, want storage error", got.Error)
	}
	shutdownOK(t, w)
}

func TestStorageTimeoutOnEachCall(t *testing.T) {
	st := store.NewMemory()
	be := backend.NewScripted()
	be.SetDelay(50 * time.Millisecond)
	sleep := func(context.Context, time.Duration) error { return nil }
	w := newTestWorker(t, st, be, sleep, Config{
		WorkerCount:     1,
		WorkerAttempts:  3,
		WorkerQueueSize: 8,
		StorageTimeout:  5 * time.Millisecond,
		RetryBaseDelay:  time.Millisecond,
	})

	createPending(t, st, "tenant-a", "snap-1", "vol-1", "nightly")
	if err := w.Enqueue(context.Background(), Job{
		TenantID:   "tenant-a",
		SnapshotID: "snap-1",
		VolumeID:   "vol-1",
		Op:         OpCreate,
	}); err != nil {
		t.Fatal(err)
	}

	got := waitStatus(t, st, "tenant-a", "snap-1", domain.StatusFailed)
	if got.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", got.Attempts)
	}
	if got.Error == "" || !strings.Contains(got.Error, context.DeadlineExceeded.Error()) {
		t.Fatalf("error = %q, want deadline exceeded", got.Error)
	}
	shutdownOK(t, w)
}

func TestBackoffIsExponential(t *testing.T) {
	st := store.NewMemory()
	be := newCaseBackend()
	be.failCreate["snap-1"] = 2
	base := 10 * time.Millisecond

	var (
		mu    sync.Mutex
		slept []time.Duration
	)
	sleep := func(_ context.Context, d time.Duration) error {
		mu.Lock()
		slept = append(slept, d)
		mu.Unlock()
		return nil
	}

	w := newTestWorker(t, st, be, sleep, Config{
		WorkerCount:     1,
		WorkerAttempts:  3,
		WorkerQueueSize: 8,
		StorageTimeout:  200 * time.Millisecond,
		RetryBaseDelay:  base,
	})

	createPending(t, st, "tenant-a", "snap-1", "vol-1", "nightly")
	if err := w.Enqueue(context.Background(), Job{
		TenantID:   "tenant-a",
		SnapshotID: "snap-1",
		VolumeID:   "vol-1",
		Op:         OpCreate,
	}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, st, "tenant-a", "snap-1", domain.StatusReady)

	mu.Lock()
	defer mu.Unlock()
	if len(slept) != 2 || slept[0] != base || slept[1] != 2*base {
		t.Fatalf("backoff sequence = %v, want [%s %s]", slept, base, 2*base)
	}
	shutdownOK(t, w)
}

func TestGlobalLimitAndVolumeLock(t *testing.T) {
	st := store.NewMemory()
	be := backend.NewScripted()
	gate := make(chan struct{})
	be.SetGate(gate)
	w := newTestWorker(t, st, be, nil, Config{
		WorkerCount:     3,
		WorkerAttempts:  3,
		WorkerQueueSize: 64,
		StorageTimeout:  time.Second,
		RetryBaseDelay:  time.Millisecond,
	})

	jobs := []Job{
		{TenantID: "t", SnapshotID: "s1", VolumeID: "vol-a", Op: OpCreate},
		{TenantID: "t", SnapshotID: "s2", VolumeID: "vol-a", Op: OpCreate},
		{TenantID: "t", SnapshotID: "s3", VolumeID: "vol-b", Op: OpCreate},
		{TenantID: "t", SnapshotID: "s4", VolumeID: "vol-c", Op: OpCreate},
		{TenantID: "t", SnapshotID: "s5", VolumeID: "vol-d", Op: OpCreate},
		{TenantID: "t", SnapshotID: "s6", VolumeID: "vol-e", Op: OpCreate},
	}
	for _, j := range jobs {
		createPending(t, st, j.TenantID, j.SnapshotID, j.VolumeID, "job-"+j.SnapshotID)
		if err := w.Enqueue(context.Background(), j); err != nil {
			t.Fatal(err)
		}
	}

	waitInflightAtLeast(t, be, 3)
	if got := be.MaxInflight(); got > 3 {
		t.Fatalf("max inflight = %d, want <= 3", got)
	}
	if got := be.MaxVolumeInflight(); got > 1 {
		t.Fatalf("max inflight per volume = %d, want <= 1", got)
	}

	close(gate)
	for _, j := range jobs {
		waitStatus(t, st, j.TenantID, j.SnapshotID, domain.StatusReady)
	}
	shutdownOK(t, w)
}

func TestVolumeLockHeldAcrossBackoffAndBusyVolumeSkipped(t *testing.T) {
	st := store.NewMemory()
	be := newCaseBackend()
	be.failCreate["snap-a1"] = 1

	sleepStarted := make(chan struct{})
	sleepRelease := make(chan struct{})
	var once sync.Once
	sleep := func(ctx context.Context, _ time.Duration) error {
		var err error
		once.Do(func() {
			close(sleepStarted)
			select {
			case <-sleepRelease:
			case <-ctx.Done():
				err = ctx.Err()
			}
		})
		return err
	}

	w := newTestWorker(t, st, be, sleep, Config{
		WorkerCount:     2,
		WorkerAttempts:  3,
		WorkerQueueSize: 16,
		StorageTimeout:  200 * time.Millisecond,
		RetryBaseDelay:  time.Millisecond,
	})

	createPending(t, st, "tenant-a", "snap-a1", "vol-a", "first")
	createPending(t, st, "tenant-a", "snap-a2", "vol-a", "second")
	createPending(t, st, "tenant-a", "snap-b1", "vol-b", "other-volume")

	if err := w.Enqueue(context.Background(), Job{TenantID: "tenant-a", SnapshotID: "snap-a1", VolumeID: "vol-a", Op: OpCreate}); err != nil {
		t.Fatal(err)
	}
	if err := w.Enqueue(context.Background(), Job{TenantID: "tenant-a", SnapshotID: "snap-a2", VolumeID: "vol-a", Op: OpCreate}); err != nil {
		t.Fatal(err)
	}
	if err := w.Enqueue(context.Background(), Job{TenantID: "tenant-a", SnapshotID: "snap-b1", VolumeID: "vol-b", Op: OpCreate}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-sleepStarted:
	case <-time.After(time.Second):
		t.Fatal("did not enter backoff")
	}

	waitStatus(t, st, "tenant-a", "snap-b1", domain.StatusReady)
	pending := getSnapshot(t, st, "tenant-a", "snap-a2")
	if pending.Status != domain.StatusPending {
		t.Fatalf("second same-volume snapshot should wait in pending, got %s", pending.Status)
	}

	close(sleepRelease)
	waitStatus(t, st, "tenant-a", "snap-a1", domain.StatusReady)
	waitStatus(t, st, "tenant-a", "snap-a2", domain.StatusReady)

	shutdownOK(t, w)
}

func TestShutdownLeavesCreatingOnCancelledStorageCall(t *testing.T) {
	st := store.NewMemory()
	be := backend.NewScripted()
	be.SetGate(make(chan struct{}))
	w := newTestWorker(t, st, be, nil, Config{
		WorkerCount:     1,
		WorkerAttempts:  3,
		WorkerQueueSize: 8,
		StorageTimeout:  time.Second,
		RetryBaseDelay:  time.Millisecond,
	})

	createPending(t, st, "tenant-a", "snap-1", "vol-1", "nightly")
	if err := w.Enqueue(context.Background(), Job{
		TenantID:   "tenant-a",
		SnapshotID: "snap-1",
		VolumeID:   "vol-1",
		Op:         OpCreate,
	}); err != nil {
		t.Fatal(err)
	}

	waitStatus(t, st, "tenant-a", "snap-1", domain.StatusCreating)
	waitInflightAtLeast(t, be, 1)

	stopCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := w.Shutdown(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown err = %v", err)
	}

	got := getSnapshot(t, st, "tenant-a", "snap-1")
	if got.Status != domain.StatusCreating {
		t.Fatalf("status = %s, want creating", got.Status)
	}
	if got.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", got.Attempts)
	}
}

func TestRequestIDFlowsIntoStorageContext(t *testing.T) {
	st := store.NewMemory()
	be := newCaseBackend()
	w := newTestWorker(t, st, be, nil, Config{
		WorkerCount:     1,
		WorkerAttempts:  3,
		WorkerQueueSize: 8,
		StorageTimeout:  200 * time.Millisecond,
		RetryBaseDelay:  time.Millisecond,
	})

	createPending(t, st, "tenant-a", "snap-1", "vol-1", "nightly")
	ctx := logger.WithRequestID(context.Background(), "req-123")
	if err := w.Enqueue(ctx, Job{
		TenantID:   "tenant-a",
		SnapshotID: "snap-1",
		VolumeID:   "vol-1",
		Op:         OpCreate,
	}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, st, "tenant-a", "snap-1", domain.StatusReady)

	ids := be.requestIDs()
	if len(ids) == 0 || ids[0] != "req-123" {
		t.Fatalf("request ids = %v, want first req-123", ids)
	}
	shutdownOK(t, w)
}

func TestParallelEnqueueRace(t *testing.T) {
	t.Parallel()

	st := store.NewMemory()
	be := backend.NewScripted()
	be.SetDelay(2 * time.Millisecond)
	w := newTestWorker(t, st, be, nil, Config{
		WorkerCount:     4,
		WorkerAttempts:  3,
		WorkerQueueSize: 256,
		StorageTimeout:  time.Second,
		RetryBaseDelay:  time.Millisecond,
	})

	const total = 48
	jobs := make([]Job, 0, total)
	for i := 0; i < total; i++ {
		id := fmtSnapshotID(i)
		vol := "vol-" + string(rune('a'+(i%8)))
		createPending(t, st, "tenant-race", id, vol, "snap-"+id)
		jobs = append(jobs, Job{
			TenantID:   "tenant-race",
			SnapshotID: id,
			VolumeID:   vol,
			Op:         OpCreate,
		})
	}

	var wg sync.WaitGroup
	wg.Add(len(jobs))
	for _, job := range jobs {
		j := job
		go func() {
			defer wg.Done()
			if err := w.Enqueue(context.Background(), j); err != nil {
				t.Errorf("enqueue %s: %v", j.SnapshotID, err)
			}
		}()
	}
	wg.Wait()

	for _, j := range jobs {
		waitStatus(t, st, j.TenantID, j.SnapshotID, domain.StatusReady)
	}
	if got := be.MaxInflight(); got > 4 {
		t.Fatalf("max inflight = %d, want <= 4", got)
	}
	if got := be.MaxVolumeInflight(); got > 1 {
		t.Fatalf("max volume inflight = %d, want <= 1", got)
	}
	shutdownOK(t, w)
}

func newTestWorker(t *testing.T, st store.Store, be backend.StorageBackend, sleep func(context.Context, time.Duration) error, cfg Config) *Worker {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	w, err := New(cfg, st, be, log)
	if err != nil {
		t.Fatal(err)
	}
	if sleep != nil {
		w.sleep = sleep
	}
	w.Start()
	return w
}

func createPending(t *testing.T, st store.Store, tenant, id, volume, name string) {
	t.Helper()
	now := time.Date(2026, 10, 8, 20, 0, 0, 0, time.UTC)
	snap := domain.Snapshot{
		ID:        id,
		TenantID:  tenant,
		VolumeID:  volume,
		Name:      name,
		Status:    domain.StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if _, created, err := st.Create(context.Background(), snap, 100, ""); err != nil || !created {
		t.Fatalf("create pending %s: created=%v err=%v", id, created, err)
	}
}

func waitStatus(t *testing.T, st store.Store, tenant, id string, want domain.Status) domain.Snapshot {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, err := st.Get(context.Background(), tenant, id)
		if err == nil && got.Status == want {
			return got
		}
		time.Sleep(2 * time.Millisecond)
	}
	got, err := st.Get(context.Background(), tenant, id)
	t.Fatalf("timeout waiting for %s on %s: %+v err=%v", want, id, got, err)
	return domain.Snapshot{}
}

func getSnapshot(t *testing.T, st store.Store, tenant, id string) domain.Snapshot {
	t.Helper()
	got, err := st.Get(context.Background(), tenant, id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	return got
}

func waitInflightAtLeast(t *testing.T, be *backend.Scripted, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if be.Inflight() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("inflight = %d, want at least %d", be.Inflight(), n)
}

func shutdownOK(t *testing.T, w *Worker) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown err = %v", err)
	}
}

func fmtSnapshotID(i int) string {
	// short stable names, no fmt import needed.
	const digits = "0123456789abcdef"
	b := []byte{'s', '-', '0', '0', '0'}
	b[2] = digits[(i>>8)&0xf]
	b[3] = digits[(i>>4)&0xf]
	b[4] = digits[i&0xf]
	return string(b)
}

type caseBackend struct {
	mu         sync.Mutex
	failCreate map[string]int
	seenIDs    []string
}

func newCaseBackend() *caseBackend {
	return &caseBackend{failCreate: make(map[string]int)}
}

func (b *caseBackend) CreateSnapshot(ctx context.Context, volumeID, snapshotID string) error {
	b.mu.Lock()
	b.seenIDs = append(b.seenIDs, logger.RequestID(ctx))
	if b.failCreate[snapshotID] > 0 {
		b.failCreate[snapshotID]--
		b.mu.Unlock()
		return backend.ErrCreateFailed
	}
	b.mu.Unlock()
	return nil
}

func (b *caseBackend) DeleteSnapshot(ctx context.Context, volumeID, snapshotID string) error {
	b.mu.Lock()
	b.seenIDs = append(b.seenIDs, logger.RequestID(ctx))
	b.mu.Unlock()
	return nil
}

func (b *caseBackend) requestIDs() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, len(b.seenIDs))
	copy(out, b.seenIDs)
	return out
}
