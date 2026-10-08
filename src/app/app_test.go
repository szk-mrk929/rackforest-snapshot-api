package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"rackforest-snapshot-api/src/app"
	"rackforest-snapshot-api/src/backend"
	"rackforest-snapshot-api/src/config"
	"rackforest-snapshot-api/src/domain"
	"rackforest-snapshot-api/src/store"
	"rackforest-snapshot-api/src/worker"
)

func TestRunShutdownWithBlockedCallLeavesConsistentState(t *testing.T) {
	be := backend.NewScripted()
	gate := make(chan struct{})
	be.SetGate(gate)
	t.Cleanup(func() { releaseGate(gate) })

	a, jobs, st := newTestApp(t, be, time.Second)
	mountProbe(a)

	createPending(t, st, "tenant-a", "snap-running", "vol-1", "running")
	createPending(t, st, "tenant-a", "snap-queued", "vol-1", "queued")
	createPending(t, st, "tenant-a", "snap-late", "vol-2", "late")
	enqueue(t, jobs, "tenant-a", "snap-running", "vol-1", worker.OpCreate)
	enqueue(t, jobs, "tenant-a", "snap-queued", "vol-1", worker.OpCreate)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- a.Run(ctx) }()

	client := testClient()
	base := "http://" + waitAddr(t, a, errCh)
	waitCode(t, client, base+"/healthz", http.StatusOK)
	waitCode(t, client, base+"/v1/snapshots", http.StatusNoContent)

	waitStatus(t, st, "tenant-a", "snap-running", domain.StatusCreating)
	waitInflight(t, be, 1)
	queued := mustGet(t, st, "tenant-a", "snap-queued")
	if queued.Status != domain.StatusPending || queued.Attempts != 0 {
		t.Fatalf("queued snapshot = %+v, want pending with no attempts", queued)
	}

	cancel()
	body := waitCode(t, client, base+"/v1/snapshots", http.StatusServiceUnavailable)
	assertShuttingDown(t, body)
	health := waitCode(t, client, base+"/healthz", http.StatusServiceUnavailable)
	assertDraining(t, health)

	waitNotAccepting(t, jobs)
	err := jobs.Enqueue(context.Background(), worker.Job{
		TenantID:   "tenant-a",
		SnapshotID: "snap-late",
		VolumeID:   "vol-2",
		Op:         worker.OpCreate,
	})
	if !errors.Is(err, domain.ErrShuttingDown) {
		t.Fatalf("enqueue during drain = %v, want shutting down", err)
	}

	if err := <-errCh; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("run = %v, want deadline exceeded", err)
	}

	running := mustGet(t, st, "tenant-a", "snap-running")
	if running.Status != domain.StatusCreating || running.Attempts != 1 || running.Error != "" {
		t.Fatalf("running snapshot = %+v, want creating after one attempt and no error", running)
	}
	queued = mustGet(t, st, "tenant-a", "snap-queued")
	if queued.Status != domain.StatusPending || queued.Attempts != 0 {
		t.Fatalf("queued snapshot after shutdown = %+v, want pending", queued)
	}
	late := mustGet(t, st, "tenant-a", "snap-late")
	if late.Status != domain.StatusPending || late.Attempts != 0 {
		t.Fatalf("rejected snapshot = %+v, want pending", late)
	}
	if jobs.Accepting() {
		t.Fatal("worker still accepting")
	}
}

func TestShutdownDeadlineLeavesDeleting(t *testing.T) {
	be := backend.NewScripted()
	gate := make(chan struct{})
	be.SetGate(gate)
	t.Cleanup(func() { releaseGate(gate) })

	a, jobs, st := newTestApp(t, be, time.Second)
	mountProbe(a)
	ts := newProbeServer(t, a)

	createPending(t, st, "tenant-a", "snap-del", "vol-1", "gone")
	markReady(t, st, "tenant-a", "snap-del")
	enqueue(t, jobs, "tenant-a", "snap-del", "vol-1", worker.OpDelete)
	waitStatus(t, st, "tenant-a", "snap-del", domain.StatusDeleting)
	waitInflight(t, be, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if err := a.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown = %v, want deadline exceeded", err)
	}

	got := mustGet(t, st, "tenant-a", "snap-del")
	if got.Status != domain.StatusDeleting || got.Attempts != 1 || got.Error != "" {
		t.Fatalf("snapshot = %+v, want deleting after one attempt and no error", got)
	}
	assertShuttingDown(t, readBody(t, testClient(), ts.URL+"/v1/snapshots", http.StatusServiceUnavailable))
	assertDraining(t, readBody(t, testClient(), ts.URL+"/healthz", http.StatusServiceUnavailable))
	if jobs.Accepting() {
		t.Fatal("worker still accepting")
	}
}

func TestShutdownWaitsForRunningCall(t *testing.T) {
	be := backend.NewScripted()
	be.SetDelay(150 * time.Millisecond)

	a, jobs, st := newTestApp(t, be, 3*time.Second)
	mountProbe(a)
	ts := newProbeServer(t, a)
	client := testClient()

	createPending(t, st, "tenant-a", "snap-fast", "vol-1", "fast")
	enqueue(t, jobs, "tenant-a", "snap-fast", "vol-1", worker.OpCreate)
	waitInflight(t, be, 1)
	readBody(t, client, ts.URL+"/v1/snapshots", http.StatusNoContent)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := a.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown = %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("shutdown took %s, want the in-flight call to finish inside the budget", elapsed)
	}

	got := mustGet(t, st, "tenant-a", "snap-fast")
	if got.Status != domain.StatusReady {
		t.Fatalf("status = %s, want ready", got.Status)
	}
	assertShuttingDown(t, readBody(t, client, ts.URL+"/v1/snapshots", http.StatusServiceUnavailable))
	assertDraining(t, readBody(t, client, ts.URL+"/healthz", http.StatusServiceUnavailable))
}

func TestNewRejectsNilWorker(t *testing.T) {
	_, err := app.New(testConfig(time.Second), slog.Default(), nil)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("new = %v", err)
	}
}

func newTestApp(t *testing.T, be backend.StorageBackend, shutdown time.Duration) (*app.App, *worker.Worker, *store.Memory) {
	t.Helper()
	st := store.NewMemory()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := testConfig(shutdown)
	jobs, err := worker.New(worker.Config{
		WorkerCount:     cfg.WorkerCount,
		WorkerAttempts:  cfg.WorkerAttempts,
		WorkerQueueSize: cfg.WorkerQueueSize,
		StorageTimeout:  cfg.StorageTimeout,
		RetryBaseDelay:  cfg.RetryBaseDelay,
	}, st, be, log)
	if err != nil {
		t.Fatal(err)
	}
	application, err := app.New(cfg, log, jobs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = application.Shutdown(ctx)
	})
	return application, jobs, st
}

func testConfig(shutdown time.Duration) config.Config {
	return config.Config{
		LogLevel:            slog.LevelInfo,
		HTTPAddress:         "127.0.0.1:0",
		ShutdownTimeout:     shutdown,
		WorkerCount:         1,
		WorkerAttempts:      3,
		WorkerQueueSize:     8,
		TenantSnapshotQuota: 10,
		StorageTimeout:      5 * time.Second,
		RetryBaseDelay:      time.Second,
		StorageMaxDelay:     time.Second,
		StoreDriver:         "memory",
	}
}

func mountProbe(a *app.App) {
	a.Mux().HandleFunc("GET /v1/snapshots", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
}

func newProbeServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	return ts
}

func createPending(t *testing.T, st store.Store, tenant, id, volume, name string) {
	t.Helper()
	snap, err := domain.NewSnapshot(id, tenant, volume, name, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := st.Create(context.Background(), snap, 10, ""); err != nil || !created {
		t.Fatalf("create %s: created=%v err=%v", id, created, err)
	}
}

func markReady(t *testing.T, st store.Store, tenant, id string) {
	t.Helper()
	now := time.Now()
	for _, next := range []domain.Status{domain.StatusCreating, domain.StatusReady} {
		_, err := st.Update(context.Background(), tenant, id, func(s domain.Snapshot) (domain.Snapshot, error) {
			if err := domain.Transition(&s, next, now); err != nil {
				return domain.Snapshot{}, err
			}
			return s, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func enqueue(t *testing.T, jobs *worker.Worker, tenant, id, volume string, op worker.Op) {
	t.Helper()
	if err := jobs.Enqueue(context.Background(), worker.Job{
		TenantID:   tenant,
		SnapshotID: id,
		VolumeID:   volume,
		Op:         op,
	}); err != nil {
		t.Fatal(err)
	}
}

func waitStatus(t *testing.T, st store.Store, tenant, id string, want domain.Status) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, err := st.Get(context.Background(), tenant, id)
		if err == nil && got.Status == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	got, _ := st.Get(context.Background(), tenant, id)
	t.Fatalf("status of %s = %s, want %s", id, got.Status, want)
}

func mustGet(t *testing.T, st store.Store, tenant, id string) domain.Snapshot {
	t.Helper()
	got, err := st.Get(context.Background(), tenant, id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	return got
}

func waitInflight(t *testing.T, be *backend.Scripted, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if be.Inflight() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("inflight = %d, want at least %d", be.Inflight(), n)
}

func waitNotAccepting(t *testing.T, jobs *worker.Worker) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if !jobs.Accepting() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("worker still accepting")
}

func waitAddr(t *testing.T, a *app.App, errCh <-chan error) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if addr := a.Addr(); addr != "" {
			return addr
		}
		select {
		case err := <-errCh:
			t.Fatalf("run: %v", err)
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
	t.Fatal("server did not listen")
	return ""
}

func testClient() *http.Client {
	return &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}
}

func waitCode(t *testing.T, client *http.Client, url string, want int) []byte {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		res, err := client.Get(url)
		if err != nil {
			last = err
			time.Sleep(5 * time.Millisecond)
			continue
		}
		body, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode == want {
			return body
		}
		last = errors.New(res.Status + " " + string(body))
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("GET %s: %v", url, last)
	return nil
}

func readBody(t *testing.T, client *http.Client, url string, want int) []byte {
	t.Helper()
	res, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != want {
		t.Fatalf("GET %s = %d %s, want %d", url, res.StatusCode, body, want)
	}
	if want != http.StatusNoContent {
		if ct := res.Header.Get("Content-Type"); ct != "application/json" {
			t.Fatalf("content type = %q", ct)
		}
	}
	return body
}

func assertHealth(t *testing.T, body []byte) {
	t.Helper()
	var got struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "ok" {
		t.Fatalf("health = %#v", got)
	}
}

func assertDraining(t *testing.T, body []byte) {
	t.Helper()
	var got struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "shutting_down" {
		t.Fatalf("health = %#v", got)
	}
}

func assertShuttingDown(t *testing.T, body []byte) {
	t.Helper()
	var got struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("body %s: %v", body, err)
	}
	if got.Error.Code != "shutting_down" || got.Error.Message != domain.ErrShuttingDown.Error() {
		t.Fatalf("error = %#v", got.Error)
	}
}

func releaseGate(gate chan struct{}) {
	select {
	case <-gate:
	default:
		close(gate)
	}
}
