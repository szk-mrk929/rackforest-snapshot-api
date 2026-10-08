package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"rackforest-snapshot-api/src/domain"
	"rackforest-snapshot-api/src/logger"
	"rackforest-snapshot-api/src/store"
	"rackforest-snapshot-api/src/worker"
)

// Server is the snapshot HTTP API. It implements the generated ServerInterface
// on the standard ServeMux. Status codes are mapped in writeDomain.
type Server struct {
	store store.Store
	jobs  *worker.Worker
	quota int
	log   *slog.Logger
	now   func() time.Time
}

// NewServer binds the store and the worker. quota is the tenant's non-deleted limit.
func NewServer(st store.Store, jobs *worker.Worker, quota int, log *slog.Logger) (*Server, error) {
	if st == nil || jobs == nil {
		return nil, fmt.Errorf("%w: store and worker are required", domain.ErrInvalidArgument)
	}
	if quota < 1 {
		return nil, fmt.Errorf("%w: quota must be positive", domain.ErrInvalidArgument)
	}
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		store: st,
		jobs:  jobs,
		quota: quota,
		log:   log.WithGroup("api"),
		now:   func() time.Time { return time.Now().UTC() },
	}, nil
}

// Handler is the standalone router, including request ids on every response.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	HandlerWithOptions(s, s.options(mux))
	mountDocs(mux)
	return withRequestID(mux)
}

// Mount registers the routes on mux. Call it before the process starts listening.
// GET /healthz is also registered; the process serves that path itself and keeps it 200 while draining.
// GET /docs serves Swagger UI for the OpenAPI document.
func (s *Server) Mount(mux *http.ServeMux) {
	HandlerWithOptions(s, s.options(mux))
	mountDocs(mux)
}

func (s *Server) options(mux ServeMux) StdHTTPServerOptions {
	return StdHTTPServerOptions{
		BaseRouter:       mux,
		Middlewares:      []MiddlewareFunc{requestIDMiddleware},
		ErrorHandlerFunc: s.writeBindError,
	}
}

// GetHealth reports the process as up. Draining keeps this 200 at the process edge.
func (s *Server) GetHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, Health{Status: Ok})
}

// CreateSnapshot admits a pending snapshot and returns 202.
// The same Idempotency-Key with the same volume and name returns 200 and the current snapshot.
func (s *Server) CreateSnapshot(w http.ResponseWriter, r *http.Request, volumeId VolumeId, params CreateSnapshotParams) {
	if !s.jobs.Accepting() {
		writeShuttingDown(w)
		return
	}
	if err := domain.ValidateScopeID("tenant_id", params.XTenantID); err != nil {
		writeDomain(r.Context(), s.log, w, err)
		return
	}
	if err := domain.ValidateScopeID("volume_id", volumeId); err != nil {
		writeDomain(r.Context(), s.log, w, err)
		return
	}
	body, err := readCreate(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	if err := domain.ValidateName(body.Name); err != nil {
		writeDomain(r.Context(), s.log, w, err)
		return
	}
	key, err := idempotencyKey(params.IdempotencyKey)
	if err != nil {
		writeDomain(r.Context(), s.log, w, err)
		return
	}

	id, err := domain.NewID()
	if err != nil {
		s.log.ErrorContext(r.Context(), "generate snapshot id", "error", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
		return
	}
	snap, err := domain.NewSnapshot(id, params.XTenantID, volumeId, body.Name, s.now())
	if err != nil {
		writeDomain(r.Context(), s.log, w, err)
		return
	}
	saved, created, err := s.store.Create(r.Context(), snap, s.quota, key)
	if err != nil {
		writeDomain(r.Context(), s.log, w, err)
		return
	}
	if !created {
		current, err := s.current(r.Context(), saved)
		if err != nil {
			writeDomain(r.Context(), s.log, w, err)
			return
		}
		if current.Status == domain.StatusPending {
			if err := s.enqueue(r.Context(), current, worker.OpCreate); err != nil && !errors.Is(err, domain.ErrInvalidState) {
				writeDomain(r.Context(), s.log, w, err)
				return
			}
			if fresh, err := s.store.Get(r.Context(), current.TenantID, current.ID); err == nil {
				current = fresh
			}
		}
		w.Header().Set("Idempotency-Replayed", "true")
		writeJSON(w, http.StatusOK, toAPI(current))
		return
	}
	if err := s.enqueue(r.Context(), saved, worker.OpCreate); err != nil {
		// The row was not queued, so it must not hold a quota slot the client never received.
		if errors.Is(err, domain.ErrQueueFull) || errors.Is(err, domain.ErrShuttingDown) {
			if rmErr := s.store.RemovePending(r.Context(), saved.TenantID, saved.ID); rmErr != nil {
				s.log.ErrorContext(r.Context(), "roll back a refused snapshot", "snapshot_id", saved.ID, "error", rmErr)
			}
		}
		writeDomain(r.Context(), s.log, w, err)
		return
	}
	s.log.InfoContext(r.Context(), "snapshot accepted", "snapshot_id", saved.ID, "volume_id", saved.VolumeID)
	writeJSON(w, http.StatusAccepted, toAPI(saved))
}

// ListSnapshots returns one tenant's snapshots, newest first.
func (s *Server) ListSnapshots(w http.ResponseWriter, r *http.Request, params ListSnapshotsParams) {
	if err := domain.ValidateScopeID("tenant_id", params.XTenantID); err != nil {
		writeDomain(r.Context(), s.log, w, err)
		return
	}
	filter, err := listFilter(params)
	if err != nil {
		writeDomain(r.Context(), s.log, w, err)
		return
	}
	snaps, err := s.store.List(r.Context(), params.XTenantID, filter)
	if err != nil {
		writeDomain(r.Context(), s.log, w, err)
		return
	}
	out := make([]Snapshot, len(snaps))
	for i := range snaps {
		out[len(snaps)-1-i] = toAPI(snaps[i])
	}
	writeJSON(w, http.StatusOK, SnapshotList{Snapshots: out})
}

// GetSnapshot returns one snapshot. Another tenant's id is not found.
func (s *Server) GetSnapshot(w http.ResponseWriter, r *http.Request, id SnapshotId, params GetSnapshotParams) {
	if err := domain.ValidateScopeID("tenant_id", params.XTenantID); err != nil {
		writeDomain(r.Context(), s.log, w, err)
		return
	}
	if err := domain.ValidateScopeID("snapshot_id", id); err != nil {
		writeDomain(r.Context(), s.log, w, err)
		return
	}
	snap, err := s.store.Get(r.Context(), params.XTenantID, id)
	if err != nil {
		writeDomain(r.Context(), s.log, w, err)
		return
	}
	writeJSON(w, http.StatusOK, toAPI(snap))
}

// DeleteSnapshot moves a deletable snapshot to deleting and returns 202.
// The worker performs the storage call after the response.
func (s *Server) DeleteSnapshot(w http.ResponseWriter, r *http.Request, id SnapshotId, params DeleteSnapshotParams) {
	if !s.jobs.Accepting() {
		writeShuttingDown(w)
		return
	}
	if err := domain.ValidateScopeID("tenant_id", params.XTenantID); err != nil {
		writeDomain(r.Context(), s.log, w, err)
		return
	}
	if err := domain.ValidateScopeID("snapshot_id", id); err != nil {
		writeDomain(r.Context(), s.log, w, err)
		return
	}
	updated, err := s.store.Update(r.Context(), params.XTenantID, id, func(snap domain.Snapshot) (domain.Snapshot, error) {
		if snap.Status == domain.StatusDeleting {
			return snap, nil
		}
		if !snap.Status.Deletable() {
			return domain.Snapshot{}, fmt.Errorf("%w: delete requires ready, failed, or error_deleting", domain.ErrInvalidState)
		}
		if err := domain.Transition(&snap, domain.StatusDeleting, s.now()); err != nil {
			return domain.Snapshot{}, err
		}
		snap.Attempts = 0
		snap.Error = ""
		return snap, nil
	})
	if err != nil {
		writeDomain(r.Context(), s.log, w, err)
		return
	}
	if err := s.enqueue(r.Context(), updated, worker.OpDelete); err != nil && !errors.Is(err, domain.ErrInvalidState) {
		writeDomain(r.Context(), s.log, w, err)
		return
	}
	s.log.InfoContext(r.Context(), "snapshot delete accepted", "snapshot_id", updated.ID)
	writeJSON(w, http.StatusAccepted, toAPI(updated))
}

func (s *Server) current(ctx context.Context, snap domain.Snapshot) (domain.Snapshot, error) {
	fresh, err := s.store.Get(ctx, snap.TenantID, snap.ID)
	if err != nil {
		return domain.Snapshot{}, err
	}
	return fresh, nil
}

func (s *Server) enqueue(ctx context.Context, snap domain.Snapshot, op worker.Op) error {
	return s.jobs.Enqueue(ctx, worker.Job{
		TenantID:   snap.TenantID,
		SnapshotID: snap.ID,
		VolumeID:   snap.VolumeID,
		Op:         op,
	})
}

func listFilter(params ListSnapshotsParams) (store.Filter, error) {
	var filter store.Filter
	if params.VolumeId != nil && *params.VolumeId != "" {
		if err := domain.ValidateScopeID("volume_id", *params.VolumeId); err != nil {
			return store.Filter{}, err
		}
		filter.VolumeID = *params.VolumeId
	}
	if params.Status != nil && *params.Status != "" {
		status := domain.Status(*params.Status)
		if !status.Valid() {
			return store.Filter{}, fmt.Errorf("%w: status %q", domain.ErrInvalidArgument, *params.Status)
		}
		filter.Status = status
	}
	return filter, nil
}

func idempotencyKey(key *IdempotencyKey) (string, error) {
	if key == nil {
		return "", nil
	}
	if err := domain.ValidateIdempotencyKey(*key); err != nil {
		return "", err
	}
	return *key, nil
}

func readCreate(w http.ResponseWriter, r *http.Request) (CreateSnapshotRequest, error) {
	if r.Body == nil {
		return CreateSnapshotRequest{}, fmt.Errorf("%w: request body is required", domain.ErrInvalidArgument)
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	var body CreateSnapshotRequest
	if err := dec.Decode(&body); err != nil {
		return CreateSnapshotRequest{}, fmt.Errorf("%w: invalid JSON", domain.ErrInvalidArgument)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return CreateSnapshotRequest{}, fmt.Errorf("%w: invalid JSON", domain.ErrInvalidArgument)
	}
	return body, nil
}

func toAPI(snap domain.Snapshot) Snapshot {
	out := Snapshot{
		Attempts:  snap.Attempts,
		CreatedAt: snap.CreatedAt.UTC(),
		Id:        snap.ID,
		Name:      snap.Name,
		Status:    SnapshotStatus(snap.Status),
		TenantId:  snap.TenantID,
		UpdatedAt: snap.UpdatedAt.UTC(),
		VolumeId:  snap.VolumeID,
	}
	if snap.Error != "" {
		msg := snap.Error
		out.Error = &msg
	}
	return out
}

// writeDomain maps a store or worker error onto the status the README lists.
// 400 is a malformed value, 404 is a missing snapshot for this tenant, 409 is
// quota, state, or an idempotency conflict, 429 is a full queue, and 503 is drain.
func writeDomain(ctx context.Context, log *slog.Logger, w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "snapshot not found")
	case errors.Is(err, domain.ErrQuotaExceeded):
		writeError(w, http.StatusConflict, "quota_exceeded", "tenant snapshot quota exceeded")
	case errors.Is(err, domain.ErrInvalidState):
		writeError(w, http.StatusConflict, "invalid_state", err.Error())
	case errors.Is(err, domain.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "idempotency_conflict", "idempotency key reused with a different request")
	case errors.Is(err, domain.ErrInvalidArgument):
		writeError(w, http.StatusBadRequest, "invalid_argument", err.Error())
	case errors.Is(err, domain.ErrShuttingDown):
		writeShuttingDown(w)
	case errors.Is(err, domain.ErrQueueFull):
		writeError(w, http.StatusTooManyRequests, "queue_full", "operation queue is full")
	default:
		log.ErrorContext(ctx, "request failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
	}
}

func (s *Server) writeBindError(w http.ResponseWriter, r *http.Request, err error) {
	var missing *RequiredHeaderError
	if errors.As(err, &missing) && missing.ParamName == "X-Tenant-ID" {
		writeError(w, http.StatusBadRequest, "missing_tenant", "X-Tenant-ID header is required")
		return
	}
	var format *InvalidParamFormatError
	if errors.As(err, &format) {
		writeError(w, http.StatusBadRequest, "invalid_argument", "invalid parameter "+format.ParamName)
		return
	}
	var tooMany *TooManyValuesForParamError
	if errors.As(err, &tooMany) {
		writeError(w, http.StatusBadRequest, "invalid_argument", tooMany.Error())
		return
	}
	writeError(w, http.StatusBadRequest, "invalid_argument", "invalid request parameter")
}

func writeShuttingDown(w http.ResponseWriter) {
	writeError(w, http.StatusServiceUnavailable, "shutting_down", domain.ErrShuttingDown.Error())
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	var body ErrorResponse
	body.Error.Code = code
	body.Error.Message = message
	writeJSON(w, status, body)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func withRequestID(next http.Handler) http.Handler {
	return requestIDMiddleware(next)
}

func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := logger.ResolveRequestID(r.Header.Get("X-Request-ID"))
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(logger.WithRequestID(r.Context(), id)))
	})
}

var _ ServerInterface = (*Server)(nil)
