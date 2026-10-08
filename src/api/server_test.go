package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"rackforest-snapshot-api/src/backend"
	"rackforest-snapshot-api/src/domain"
	"rackforest-snapshot-api/src/store"
	"rackforest-snapshot-api/src/worker"
)

func TestHealthDoesNotNeedATenant(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	res := h.do(t, http.MethodGet, "/healthz", "", nil, nil)
	if res.code != http.StatusOK {
		t.Fatalf("status = %d, body %s", res.code, res.raw)
	}
	var health Health
	if err := json.Unmarshal(res.raw, &health); err != nil {
		t.Fatal(err)
	}
	if health.Status != Ok {
		t.Fatalf("status = %s", health.Status)
	}
}

func TestMissingTenantIsBadRequest(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	res := h.do(t, http.MethodPost, "/v1/volumes/vol-1/snapshots", "", []byte(`{"name":"nightly"}`), nil)
	assertError(t, res, http.StatusBadRequest, "missing_tenant")
}

func TestCreateListGetAndIdempotency(t *testing.T) {
	h := newHarness(t, harnessOpts{})

	first := h.create(t, "tenant-a", "vol-1", "nightly", "key-1")
	if first.code != http.StatusAccepted {
		t.Fatalf("create = %d %s", first.code, first.raw)
	}
	if first.snap.Status != Pending || first.snap.Name != "nightly" || first.snap.VolumeId != "vol-1" || first.snap.TenantId != "tenant-a" {
		t.Fatalf("accepted = %+v", first.snap)
	}
	if first.header.Get("Idempotency-Replayed") != "" {
		t.Fatal("first create was marked replayed")
	}
	waitStatus(t, h, "tenant-a", first.snap.Id, Ready)

	replay := h.create(t, "tenant-a", "vol-1", "nightly", "key-1")
	if replay.code != http.StatusOK {
		t.Fatalf("replay = %d %s", replay.code, replay.raw)
	}
	if replay.header.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay header = %q", replay.header.Get("Idempotency-Replayed"))
	}
	if replay.snap.Id != first.snap.Id || replay.snap.Status != Ready {
		t.Fatalf("replay = %+v", replay.snap)
	}

	conflict := h.create(t, "tenant-a", "vol-1", "other", "key-1")
	assertError(t, conflict, http.StatusConflict, "idempotency_conflict")
	otherVolume := h.create(t, "tenant-a", "vol-2", "nightly", "key-1")
	assertError(t, otherVolume, http.StatusConflict, "idempotency_conflict")

	second := h.create(t, "tenant-a", "vol-2", "weekly", "")
	if second.code != http.StatusAccepted {
		t.Fatalf("second = %d %s", second.code, second.raw)
	}
	waitStatus(t, h, "tenant-a", second.snap.Id, Ready)

	sameKeyOtherTenant := h.create(t, "tenant-b", "vol-9", "nightly", "key-1")
	if sameKeyOtherTenant.code != http.StatusAccepted || sameKeyOtherTenant.snap.Id == first.snap.Id {
		t.Fatalf("other tenant = %d %+v", sameKeyOtherTenant.code, sameKeyOtherTenant.snap)
	}

	listed := h.do(t, http.MethodGet, "/v1/snapshots", "tenant-a", nil, nil)
	if listed.code != http.StatusOK {
		t.Fatalf("list = %d %s", listed.code, listed.raw)
	}
	if len(listed.list.Snapshots) != 2 || listed.list.Snapshots[0].Id != second.snap.Id || listed.list.Snapshots[1].Id != first.snap.Id {
		t.Fatalf("list = %+v", listed.list.Snapshots)
	}

	filtered := h.do(t, http.MethodGet, "/v1/snapshots?volume_id=vol-1&status=ready", "tenant-a", nil, nil)
	if filtered.code != http.StatusOK || len(filtered.list.Snapshots) != 1 || filtered.list.Snapshots[0].Id != first.snap.Id {
		t.Fatalf("filtered = %d %+v", filtered.code, filtered.list.Snapshots)
	}
	badStatus := h.do(t, http.MethodGet, "/v1/snapshots?status=nope", "tenant-a", nil, nil)
	assertError(t, badStatus, http.StatusBadRequest, "invalid_argument")

	got := h.do(t, http.MethodGet, "/v1/snapshots/"+first.snap.Id, "tenant-a", nil, nil)
	if got.code != http.StatusOK || got.snap.Id != first.snap.Id {
		t.Fatalf("get = %d %+v", got.code, got.snap)
	}
	foreign := h.do(t, http.MethodGet, "/v1/snapshots/"+first.snap.Id, "tenant-b", nil, nil)
	assertError(t, foreign, http.StatusNotFound, "not_found")
	missing := h.do(t, http.MethodGet, "/v1/snapshots/missing-snap", "tenant-a", nil, nil)
	assertError(t, missing, http.StatusNotFound, "not_found")

	bad := h.do(t, http.MethodPost, "/v1/volumes/vol-1/snapshots", "tenant-a", []byte(`{"name":"ok","extra":1}`), nil)
	assertError(t, bad, http.StatusBadRequest, "invalid_argument")
	empty := h.do(t, http.MethodPost, "/v1/volumes/vol-1/snapshots", "tenant-a", []byte(`{"name":""}`), nil)
	assertError(t, empty, http.StatusBadRequest, "invalid_argument")
}

func TestQuotaIgnoresDeleted(t *testing.T) {
	h := newHarness(t, harnessOpts{quota: 1})
	first := h.create(t, "tenant-a", "vol-1", "one", "k1")
	if first.code != http.StatusAccepted {
		t.Fatalf("create = %d %s", first.code, first.raw)
	}
	second := h.create(t, "tenant-a", "vol-1", "two", "k2")
	assertError(t, second, http.StatusConflict, "quota_exceeded")
	replay := h.create(t, "tenant-a", "vol-1", "one", "k1")
	if replay.code != http.StatusOK || replay.snap.Id != first.snap.Id {
		t.Fatalf("replay over quota = %d %+v", replay.code, replay.snap)
	}

	waitStatus(t, h, "tenant-a", first.snap.Id, Ready)
	deleted := h.do(t, http.MethodDelete, "/v1/snapshots/"+first.snap.Id, "tenant-a", nil, nil)
	if deleted.code != http.StatusAccepted || deleted.snap.Status != Deleting {
		t.Fatalf("delete = %d %+v", deleted.code, deleted.snap)
	}
	waitStatus(t, h, "tenant-a", first.snap.Id, Deleted)

	third := h.create(t, "tenant-a", "vol-1", "three", "k3")
	if third.code != http.StatusAccepted {
		t.Fatalf("create after delete = %d %s", third.code, third.raw)
	}
}

func TestDeleteRejectsInProgressCreate(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	gate := make(chan struct{})
	h.be.SetGate(gate)
	t.Cleanup(func() { releaseGate(gate) })

	created := h.create(t, "tenant-a", "vol-1", "nightly", "")
	if created.code != http.StatusAccepted {
		t.Fatalf("create = %d %s", created.code, created.raw)
	}
	waitStatus(t, h, "tenant-a", created.snap.Id, Creating)

	res := h.do(t, http.MethodDelete, "/v1/snapshots/"+created.snap.Id, "tenant-a", nil, nil)
	assertError(t, res, http.StatusConflict, "invalid_state")
	releaseGate(gate)
	waitStatus(t, h, "tenant-a", created.snap.Id, Ready)
}

func TestDeleteStaysDeletingUntilStorageFinishes(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	created := h.create(t, "tenant-a", "vol-1", "nightly", "")
	waitStatus(t, h, "tenant-a", created.snap.Id, Ready)

	gate := make(chan struct{})
	h.be.SetGate(gate)
	t.Cleanup(func() { releaseGate(gate) })

	res := h.do(t, http.MethodDelete, "/v1/snapshots/"+created.snap.Id, "tenant-a", nil, nil)
	if res.code != http.StatusAccepted || res.snap.Status != Deleting || res.snap.Attempts != 0 {
		t.Fatalf("delete = %d %+v", res.code, res.snap)
	}
	got := h.do(t, http.MethodGet, "/v1/snapshots/"+created.snap.Id, "tenant-a", nil, nil)
	if got.snap.Status != Deleting {
		t.Fatalf("while storage is blocked, status = %s", got.snap.Status)
	}
	releaseGate(gate)
	waitStatus(t, h, "tenant-a", created.snap.Id, Deleted)
}

func TestQueueFullAndShutdown(t *testing.T) {
	h := newHarness(t, harnessOpts{workers: 1, queue: 1})
	gate := make(chan struct{})
	h.be.SetGate(gate)
	t.Cleanup(func() { releaseGate(gate) })

	first := h.create(t, "tenant-a", "vol-1", "one", "")
	if first.code != http.StatusAccepted {
		t.Fatalf("first = %d %s", first.code, first.raw)
	}
	waitStatus(t, h, "tenant-a", first.snap.Id, Creating)

	second := h.create(t, "tenant-a", "vol-1", "two", "")
	if second.code != http.StatusAccepted || second.snap.Status != Pending {
		t.Fatalf("queued = %d %+v", second.code, second.snap)
	}
	third := h.create(t, "tenant-a", "vol-1", "three", "refused")
	assertError(t, third, http.StatusTooManyRequests, "queue_full")
	listed := h.do(t, http.MethodGet, "/v1/snapshots", "tenant-a", nil, nil)
	if listed.code != http.StatusOK || len(listed.list.Snapshots) != 2 {
		t.Fatalf("refused create stayed in the list: %+v", listed.list.Snapshots)
	}

	releaseGate(gate)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.jobs.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	res := h.create(t, "tenant-a", "vol-2", "late", "late")
	assertError(t, res, http.StatusServiceUnavailable, "shutting_down")
}

func TestDocsServesSwaggerUI(t *testing.T) {
	h := newHarness(t, harnessOpts{})

	page := h.do(t, http.MethodGet, "/docs", "", nil, nil)
	if page.code != http.StatusOK || !strings.Contains(string(page.raw), "swagger-ui-bundle.js") || !strings.Contains(string(page.raw), "RackForest Snapshot API") {
		t.Fatalf("docs = %d %s", page.code, page.raw)
	}
	slash := h.do(t, http.MethodGet, "/docs/", "", nil, nil)
	if slash.code != http.StatusOK || !strings.Contains(string(slash.raw), "swagger-ui.css") {
		t.Fatalf("docs/ = %d", slash.code)
	}

	css := h.do(t, http.MethodGet, "/docs/swagger-ui.css", "", nil, nil)
	if css.code != http.StatusOK || !strings.Contains(css.header.Get("Content-Type"), "text/css") || len(css.raw) == 0 {
		t.Fatalf("css = %d %s len %d", css.code, css.header.Get("Content-Type"), len(css.raw))
	}

	spec := h.do(t, http.MethodGet, "/docs/openapi.json", "", nil, nil)
	if spec.code != http.StatusOK || !strings.Contains(spec.header.Get("Content-Type"), "application/json") {
		t.Fatalf("spec = %d %s", spec.code, spec.header.Get("Content-Type"))
	}
	body := string(spec.raw)
	for _, want := range []string{"/healthz", "/v1/snapshots", "/v1/volumes/{volumeId}/snapshots", "RackForest Snapshot API"} {
		if !strings.Contains(body, want) {
			t.Fatalf("spec missing %q", want)
		}
	}
}

func TestRequestID(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	kept := h.do(t, http.MethodGet, "/healthz", "", nil, map[string]string{"X-Request-ID": "req-42"})
	if kept.header.Get("X-Request-ID") != "req-42" {
		t.Fatalf("request id = %q", kept.header.Get("X-Request-ID"))
	}
	replaced := h.do(t, http.MethodGet, "/healthz", "", nil, map[string]string{"X-Request-ID": "bad id"})
	got := replaced.header.Get("X-Request-ID")
	if got == "" || got == "bad id" || strings.ContainsAny(got, " ") {
		t.Fatalf("request id = %q", got)
	}
}

type harnessOpts struct {
	quota   int
	workers int
	queue   int
}

type harness struct {
	ts   *httptest.Server
	be   *backend.Scripted
	jobs *worker.Worker
}

type response struct {
	code   int
	header http.Header
	raw    []byte
	snap   Snapshot
	list   SnapshotList
}

func newHarness(t *testing.T, opts harnessOpts) *harness {
	t.Helper()
	if opts.quota == 0 {
		opts.quota = 10
	}
	if opts.workers == 0 {
		opts.workers = 2
	}
	if opts.queue == 0 {
		opts.queue = 8
	}
	be := backend.NewScripted()
	st := store.NewMemory()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	jobs, err := worker.New(worker.Config{
		WorkerCount:     opts.workers,
		WorkerAttempts:  3,
		WorkerQueueSize: opts.queue,
		StorageTimeout:  2 * time.Second,
		RetryBaseDelay:  5 * time.Millisecond,
	}, st, be, log)
	if err != nil {
		t.Fatal(err)
	}
	jobs.Start()
	srv, err := NewServer(st, jobs, opts.quota, log)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ts.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = jobs.Shutdown(ctx)
	})
	return &harness{ts: ts, be: be, jobs: jobs}
}

func (h *harness) create(t *testing.T, tenant, volume, name, key string) response {
	t.Helper()
	payload, err := json.Marshal(CreateSnapshotRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	header := map[string]string{}
	if key != "" {
		header["Idempotency-Key"] = key
	}
	return h.do(t, http.MethodPost, "/v1/volumes/"+volume+"/snapshots", tenant, payload, header)
}

func (h *harness) do(t *testing.T, method, path, tenant string, body []byte, header map[string]string) response {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, h.ts.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if tenant != "" {
		req.Header.Set("X-Tenant-ID", tenant)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	res, err := h.ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	out := response{code: res.StatusCode, header: res.Header.Clone(), raw: raw}
	_ = json.Unmarshal(raw, &out.snap)
	_ = json.Unmarshal(raw, &out.list)
	return out
}

func waitStatus(t *testing.T, h *harness, tenant, id string, want SnapshotStatus) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var last response
	for time.Now().Before(deadline) {
		last = h.do(t, http.MethodGet, "/v1/snapshots/"+id, tenant, nil, nil)
		if last.code == http.StatusOK && last.snap.Status == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("status of %s = %d %s, want %s", id, last.code, last.raw, want)
}

func assertError(t *testing.T, res response, status int, code string) {
	t.Helper()
	if res.code != status {
		t.Fatalf("status = %d, want %d, body %s", res.code, status, res.raw)
	}
	if ct := res.header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type = %q", ct)
	}
	var body ErrorResponse
	if err := json.Unmarshal(res.raw, &body); err != nil {
		t.Fatalf("body %s: %v", res.raw, err)
	}
	if body.Error.Code != code || body.Error.Message == "" {
		t.Fatalf("error = %+v, want code %s", body.Error, code)
	}
}

func releaseGate(gate chan struct{}) {
	select {
	case <-gate:
	default:
		close(gate)
	}
}

func TestNewServerRejectsNil(t *testing.T) {
	_, err := NewServer(nil, nil, 1, nil)
	if err == nil || !strings.Contains(err.Error(), domain.ErrInvalidArgument.Error()) {
		t.Fatalf("new = %v", err)
	}
}
