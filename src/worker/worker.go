// Package worker runs snapshot creates and deletes off the request path.
//
// Enqueue only admits a job. The snapshot stays pending until a storage call
// is about to start, and only then moves to creating or deleting. The global
// limit is a semaphore held for that call alone, so exponential backoff does
// not occupy a slot. The volume lock covers the whole retry series, backoff
// included, and a busy volume is skipped so the FIFO queue can run another.
// Cancelling a call because the worker is stopping leaves the snapshot
// creating or deleting; it does not mark the operation failed.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"rackforest-snapshot-api/src/backend"
	"rackforest-snapshot-api/src/domain"
	"rackforest-snapshot-api/src/logger"
	"rackforest-snapshot-api/src/store"
	"rackforest-snapshot-api/src/utils/ctxutil"
)

const (
	// OpCreate asks storage to create the snapshot.
	OpCreate Op = "create"
	// OpDelete asks storage to delete the snapshot.
	OpDelete Op = "delete"
)

// Op is the storage operation a job performs.
type Op string

// Job is one admitted operation. SnapshotID is the storage identifier.
// RequestID is copied from the enqueue context so later logs keep it.
type Job struct {
	TenantID   string
	SnapshotID string
	VolumeID   string
	Op         Op
	RequestID  string
}

func (j Job) key() string {
	return j.TenantID + "\x00" + j.SnapshotID + "\x00" + string(j.Op)
}

func (j Job) validate() error {
	if err := domain.ValidateScopeID("tenant_id", j.TenantID); err != nil {
		return err
	}
	if err := domain.ValidateScopeID("snapshot_id", j.SnapshotID); err != nil {
		return err
	}
	if err := domain.ValidateScopeID("volume_id", j.VolumeID); err != nil {
		return err
	}
	if j.Op != OpCreate && j.Op != OpDelete {
		return fmt.Errorf("%w: unknown operation %q", domain.ErrInvalidArgument, j.Op)
	}
	return nil
}

// Config is the admission and retry policy. Values match the process config:
// WorkerCount is the global cap, held only while a storage call runs.
type Config struct {
	WorkerCount     int
	WorkerAttempts  int
	WorkerQueueSize int
	StorageTimeout  time.Duration
	RetryBaseDelay  time.Duration
}

// Worker is the in-process scheduler.
type Worker struct {
	cfg     Config
	store   store.Store
	backend backend.StorageBackend
	log     *slog.Logger
	now     func() time.Time
	sleep   func(ctx context.Context, d time.Duration) error

	sem chan struct{}

	mu           sync.Mutex
	queue        []Job
	active       map[string]struct{}
	busy         map[string]struct{}
	accepting    bool
	root         context.Context
	retry        context.Context
	stopRoot     context.CancelFunc
	stopRetry    context.CancelFunc
	flight       sync.WaitGroup
	shutdownOnce sync.Once
	shutdownErr  error
	rec          Recorder
}

// Recorder receives queue depth and one sample per finished storage call.
// A nil recorder is ignored. Set it before Start.
type Recorder interface {
	SetQueueDepth(n int)
	ObserveOperation(op string, d time.Duration, failed bool)
}

// New builds a worker that rejects jobs until Start.
func New(cfg Config, st store.Store, be backend.StorageBackend, log *slog.Logger) (*Worker, error) {
	if st == nil || be == nil {
		return nil, fmt.Errorf("%w: store and backend are required", domain.ErrInvalidArgument)
	}
	if cfg.WorkerCount < 1 || cfg.WorkerAttempts < 1 || cfg.WorkerQueueSize < 1 {
		return nil, fmt.Errorf("%w: worker count, attempts, and queue size must be positive", domain.ErrInvalidArgument)
	}
	if cfg.StorageTimeout <= 0 || cfg.RetryBaseDelay <= 0 {
		return nil, fmt.Errorf("%w: storage timeout and retry base delay must be positive", domain.ErrInvalidArgument)
	}
	if log == nil {
		log = slog.Default()
	}
	return &Worker{
		cfg:     cfg,
		store:   st,
		backend: be,
		log:     log.WithGroup("worker"),
		now:     func() time.Time { return time.Now().UTC() },
		sleep:   ctxutil.Sleep,
		sem:     make(chan struct{}, cfg.WorkerCount),
		active:  make(map[string]struct{}),
		busy:    make(map[string]struct{}),
	}, nil
}

// SetRecorder attaches metrics. Call it before Start.
func (w *Worker) SetRecorder(rec Recorder) {
	w.mu.Lock()
	w.rec = rec
	w.mu.Unlock()
}

// Start admits jobs and owns the context later storage calls derive from.
func (w *Worker) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopRoot != nil {
		return
	}
	root, stopRoot := context.WithCancel(context.Background())
	retry, stopRetry := context.WithCancel(root)
	w.root = root
	w.retry = retry
	w.stopRoot = stopRoot
	w.stopRetry = stopRetry
	w.accepting = true
}

// Accepting reports whether Enqueue still admits jobs.
func (w *Worker) Accepting() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.accepting
}

// Enqueue admits a job. The same snapshot and operation, already queued or
// running, is ignored so a repeated request does not run it twice.
//
// Create requires pending. Delete requires a deletable status, or deleting
// when the caller already moved it. A caller that moves a snapshot to
// deleting before enqueue must leave Attempts at 0 so this series starts at 1.
// The volume lock key is the volume id alone, shared by every tenant.
func (w *Worker) Enqueue(ctx context.Context, job Job) error {
	return w.enqueue(ctx, job, false)
}

// Recover enqueues snapshots left pending, creating, or deleting.
// The storage id is the snapshot id, and both storage calls are idempotent,
// so a create that already landed becomes ready and a delete that already
// landed becomes deleted. Attempts already stored are kept, so a restart
// does not grant a fresh budget and a timed-out call is not failed here.
// Recovery ignores the queue cap: dropping an in-flight snapshot would hide it.
func (w *Worker) Recover(ctx context.Context) error {
	snaps, err := w.store.Recoverable(ctx)
	if err != nil {
		return err
	}
	for _, snap := range snaps {
		op := OpCreate
		if snap.Status == domain.StatusDeleting {
			op = OpDelete
		}
		err := w.enqueue(ctx, Job{
			TenantID:   snap.TenantID,
			SnapshotID: snap.ID,
			VolumeID:   snap.VolumeID,
			Op:         op,
		}, true)
		if err != nil {
			return fmt.Errorf("recover snapshot %s: %w", snap.ID, err)
		}
		w.log.InfoContext(ctx, "recovered snapshot", "snapshot_id", snap.ID, "status", snap.Status, "op", op)
	}
	return nil
}

func (w *Worker) enqueue(ctx context.Context, job Job, resume bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := job.validate(); err != nil {
		return err
	}
	job.RequestID = logger.RequestID(ctx)

	w.mu.Lock()
	if _, ok := w.active[job.key()]; ok {
		w.mu.Unlock()
		return nil
	}
	if !w.accepting {
		w.mu.Unlock()
		return domain.ErrShuttingDown
	}
	w.mu.Unlock()

	snap, err := w.store.Get(ctx, job.TenantID, job.SnapshotID)
	if err != nil {
		return err
	}
	if snap.VolumeID != job.VolumeID {
		return fmt.Errorf("%w: volume_id does not match the snapshot", domain.ErrInvalidArgument)
	}
	if err := admit(job.Op, snap.Status); err != nil && !(resume && resumable(job.Op, snap.Status)) {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.active[job.key()]; ok {
		return nil
	}
	if !w.accepting {
		return domain.ErrShuttingDown
	}
	if !resume && len(w.queue) >= w.cfg.WorkerQueueSize {
		return domain.ErrQueueFull
	}
	w.queue = append(w.queue, job)
	w.active[job.key()] = struct{}{}
	w.kickLocked()
	return nil
}

func resumable(op Op, status domain.Status) bool {
	switch op {
	case OpCreate:
		return status == domain.StatusPending || status == domain.StatusCreating
	case OpDelete:
		return status == domain.StatusDeleting || status.Deletable()
	default:
		return false
	}
}

func admit(op Op, status domain.Status) error {
	switch op {
	case OpCreate:
		if status != domain.StatusPending {
			return fmt.Errorf("%w: create requires pending", domain.ErrInvalidState)
		}
	case OpDelete:
		if status != domain.StatusDeleting && !status.Deletable() {
			return fmt.Errorf("%w: delete requires ready, failed, or error_deleting", domain.ErrInvalidState)
		}
	default:
		return fmt.Errorf("%w: unknown operation %q", domain.ErrInvalidArgument, op)
	}
	return nil
}

// Shutdown stops admission, drops queued work, and waits for the storage call
// already running. retry is cancelled immediately so backoff and a job still
// waiting for a slot do not start another call. root is cancelled only when
// ctx expires, which aborts that running call and leaves creating or deleting.
// Queued snapshots stay pending.
func (w *Worker) Shutdown(ctx context.Context) error {
	w.shutdownOnce.Do(func() {
		w.shutdownErr = w.shutdown(ctx)
	})
	return w.shutdownErr
}

func (w *Worker) shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	w.mu.Lock()
	w.accepting = false
	stopRetry := w.stopRetry
	stopRoot := w.stopRoot
	w.mu.Unlock()
	if stopRetry != nil {
		stopRetry()
	}

	done := make(chan struct{})
	go func() {
		w.flight.Wait()
		close(done)
	}()

	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = ctx.Err()
	}
	if stopRoot != nil {
		stopRoot()
	}
	<-done
	return err
}

// kickLocked starts every leading job whose volume is free.
// The caller holds w.mu. Jobs past a busy volume stay in order and are skipped.
func (w *Worker) kickLocked() {
	defer w.noteQueueLocked()
	if !w.accepting {
		return
	}
	for {
		idx := -1
		for i, job := range w.queue {
			if _, taken := w.busy[job.VolumeID]; taken {
				continue
			}
			idx = i
			break
		}
		if idx < 0 {
			return
		}
		job := w.queue[idx]
		w.queue = append(w.queue[:idx], w.queue[idx+1:]...)
		w.busy[job.VolumeID] = struct{}{}
		w.flight.Add(1)
		go w.run(job)
	}
}

func (w *Worker) run(job Job) {
	defer func() {
		w.mu.Lock()
		delete(w.busy, job.VolumeID)
		delete(w.active, job.key())
		w.kickLocked()
		w.mu.Unlock()
		w.flight.Done()
	}()
	w.execute(job)
}

func (w *Worker) execute(job Job) {
	for {
		if err := w.acquire(); err != nil {
			w.log.WarnContext(w.logCtx(job), "leaving snapshot before the storage call",
				"snapshot_id", job.SnapshotID, "op", job.Op, "error", err)
			return
		}
		again, err := w.attempt(job)
		w.release()
		if err != nil || !again {
			return
		}
		snap, loadErr := w.load(job)
		if loadErr != nil {
			w.log.ErrorContext(w.logCtx(job), "reload snapshot after a storage failure",
				"snapshot_id", job.SnapshotID, "error", loadErr)
			return
		}
		if err := w.sleep(w.retryContext(), backoffDelay(w.cfg.RetryBaseDelay, snap.Attempts)); err != nil {
			w.log.WarnContext(w.logCtx(job), "retry interrupted",
				"snapshot_id", job.SnapshotID, "op", job.Op, "error", err)
			return
		}
	}
}

// attempt runs one storage call. again is true when the series should back off
// and try once more. The semaphore is held by the caller across this method
// and released before that backoff.
func (w *Worker) attempt(job Job) (again bool, err error) {
	if err := w.begin(job); err != nil {
		if errors.Is(err, errExhausted) {
			w.fail(job, "attempts exhausted")
			return false, nil
		}
		if errors.Is(err, errNotRunnable) {
			w.log.WarnContext(w.logCtx(job), "snapshot is not runnable",
				"snapshot_id", job.SnapshotID, "op", job.Op)
			return false, nil
		}
		w.log.ErrorContext(w.logCtx(job), "record storage attempt",
			"snapshot_id", job.SnapshotID, "op", job.Op, "error", err)
		return false, err
	}

	started := w.now()
	callErr := w.invoke(job)
	if !w.abandoned(callErr) {
		w.observe(job, w.now().Sub(started), callErr != nil)
	}
	if callErr == nil {
		w.succeed(job)
		w.log.InfoContext(w.logCtx(job), "snapshot operation finished",
			"snapshot_id", job.SnapshotID, "volume_id", job.VolumeID, "op", job.Op)
		return false, nil
	}
	if w.abandoned(callErr) {
		w.log.WarnContext(w.logCtx(job), "snapshot operation abandoned",
			"snapshot_id", job.SnapshotID, "op", job.Op, "error", callErr)
		return false, nil
	}

	snap, err := w.load(job)
	if err != nil {
		w.log.ErrorContext(w.logCtx(job), "reload snapshot after a storage failure",
			"snapshot_id", job.SnapshotID, "error", err)
		return false, err
	}
	w.log.WarnContext(w.logCtx(job), "storage call failed",
		"snapshot_id", job.SnapshotID, "volume_id", job.VolumeID, "op", job.Op,
		"attempt", snap.Attempts, "error", callErr)
	if snap.Attempts >= w.cfg.WorkerAttempts || w.stopping() {
		if snap.Attempts >= w.cfg.WorkerAttempts {
			w.fail(job, callErr.Error())
		} else if noteErr := w.note(job, callErr.Error()); noteErr != nil {
			w.log.ErrorContext(w.logCtx(job), "record storage error",
				"snapshot_id", job.SnapshotID, "error", noteErr)
		}
		return false, nil
	}
	if noteErr := w.note(job, callErr.Error()); noteErr != nil {
		w.log.ErrorContext(w.logCtx(job), "record storage error",
			"snapshot_id", job.SnapshotID, "error", noteErr)
	}
	return true, nil
}

// begin moves a pending or deletable snapshot into the in-progress status and
// records the attempt. It runs only after a semaphore slot is held, so the
// status changes when storage is about to be called.
func (w *Worker) begin(job Job) error {
	now := w.now()
	_, err := w.store.Update(context.Background(), job.TenantID, job.SnapshotID, func(s domain.Snapshot) (domain.Snapshot, error) {
		switch job.Op {
		case OpCreate:
			switch s.Status {
			case domain.StatusPending:
				if err := domain.Transition(&s, domain.StatusCreating, now); err != nil {
					return domain.Snapshot{}, err
				}
				s.Attempts = 1
				s.Error = ""
				return s, nil
			case domain.StatusCreating:
				return nextAttempt(s, w.cfg.WorkerAttempts, now)
			default:
				return domain.Snapshot{}, errNotRunnable
			}
		case OpDelete:
			switch s.Status {
			case domain.StatusReady, domain.StatusFailed, domain.StatusErrorDeleting:
				if err := domain.Transition(&s, domain.StatusDeleting, now); err != nil {
					return domain.Snapshot{}, err
				}
				s.Attempts = 1
				s.Error = ""
				return s, nil
			case domain.StatusDeleting:
				if s.Attempts == 0 {
					s.Attempts = 1
					s.Error = ""
					s.UpdatedAt = now.UTC()
					return s, nil
				}
				return nextAttempt(s, w.cfg.WorkerAttempts, now)
			default:
				return domain.Snapshot{}, errNotRunnable
			}
		default:
			return domain.Snapshot{}, errNotRunnable
		}
	})
	return err
}

func nextAttempt(s domain.Snapshot, max int, now time.Time) (domain.Snapshot, error) {
	if s.Attempts >= max {
		return domain.Snapshot{}, errExhausted
	}
	s.Attempts++
	s.Error = ""
	s.UpdatedAt = now.UTC()
	return s, nil
}

func (w *Worker) invoke(job Job) error {
	ctx, cancel := context.WithTimeout(w.rootContext(), w.cfg.StorageTimeout)
	defer cancel()
	ctx = logger.WithRequestID(ctx, job.RequestID)
	switch job.Op {
	case OpCreate:
		return w.backend.CreateSnapshot(ctx, job.VolumeID, job.SnapshotID)
	case OpDelete:
		return w.backend.DeleteSnapshot(ctx, job.VolumeID, job.SnapshotID)
	default:
		return fmt.Errorf("%w: unknown operation %q", domain.ErrInvalidArgument, job.Op)
	}
}

func (w *Worker) succeed(job Job) {
	to := domain.StatusReady
	if job.Op == OpDelete {
		to = domain.StatusDeleted
	}
	w.finish(job, to, "")
}

func (w *Worker) fail(job Job, msg string) {
	to := domain.StatusFailed
	if job.Op == OpDelete {
		to = domain.StatusErrorDeleting
	}
	w.finish(job, to, msg)
}

func (w *Worker) finish(job Job, to domain.Status, msg string) {
	now := w.now()
	_, err := w.store.Update(context.Background(), job.TenantID, job.SnapshotID, func(s domain.Snapshot) (domain.Snapshot, error) {
		if err := domain.Transition(&s, to, now); err != nil {
			return domain.Snapshot{}, err
		}
		s.Error = msg
		return s, nil
	})
	if err != nil {
		w.log.ErrorContext(w.logCtx(job), "persist snapshot result",
			"snapshot_id", job.SnapshotID, "status", to, "error", err)
	}
}

func (w *Worker) note(job Job, msg string) error {
	expect := domain.StatusCreating
	if job.Op == OpDelete {
		expect = domain.StatusDeleting
	}
	now := w.now()
	_, err := w.store.Update(context.Background(), job.TenantID, job.SnapshotID, func(s domain.Snapshot) (domain.Snapshot, error) {
		if s.Status != expect {
			return domain.Snapshot{}, errNotRunnable
		}
		s.Error = msg
		s.UpdatedAt = now.UTC()
		return s, nil
	})
	return err
}

func (w *Worker) load(job Job) (domain.Snapshot, error) {
	return w.store.Get(context.Background(), job.TenantID, job.SnapshotID)
}

func (w *Worker) acquire() error {
	retry := w.retryContext()
	select {
	case <-retry.Done():
		return retry.Err()
	case w.sem <- struct{}{}:
		return nil
	}
}

func (w *Worker) release() {
	<-w.sem
}

func (w *Worker) abandoned(err error) bool {
	if w.rootContext().Err() == nil {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (w *Worker) stopping() bool {
	return w.retryContext().Err() != nil
}

func (w *Worker) rootContext() context.Context {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.root == nil {
		return context.Background()
	}
	return w.root
}

func (w *Worker) retryContext() context.Context {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.retry == nil {
		return context.Background()
	}
	return w.retry
}

func (w *Worker) noteQueueLocked() {
	if w.rec != nil {
		w.rec.SetQueueDepth(len(w.queue))
	}
}

func (w *Worker) observe(opJob Job, d time.Duration, failed bool) {
	w.mu.Lock()
	rec := w.rec
	w.mu.Unlock()
	if rec != nil {
		rec.ObserveOperation(string(opJob.Op), d, failed)
	}
}

func (w *Worker) logCtx(job Job) context.Context {
	return logger.WithRequestID(context.Background(), job.RequestID)
}

// backoffDelay is base * 2^(failedAttempt-1). failedAttempt is the attempt
// that just failed, starting at 1, so the first wait is base and the next is
// twice that. There is no wait after the final attempt.
func backoffDelay(base time.Duration, failedAttempt int) time.Duration {
	if base <= 0 || failedAttempt < 1 {
		return 0
	}
	delay := base
	for i := 1; i < failedAttempt; i++ {
		if delay > time.Duration(1<<62) {
			return time.Duration(1 << 62)
		}
		delay *= 2
	}
	return delay
}

var (
	errNotRunnable = errors.New("snapshot is not runnable")
	errExhausted   = errors.New("attempts exhausted")
)
