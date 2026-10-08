package backend

import (
	"context"
	"sync"
	"time"

	"rackforest-snapshot-api/src/logger"
	"rackforest-snapshot-api/src/utils/ctxutil"
)

// Scripted is a StorageBackend whose delay, failures, and blocking are set by tests.
// It records how many calls overlap so a test can assert the concurrency limits.
type Scripted struct {
	mu sync.Mutex

	delay       time.Duration
	gate        chan struct{}
	failCreates int
	failDeletes int

	inflight    int
	maxInflight int
	vol         map[string]int
	maxVol      int
	creates     int
	deletes     int
	requestIDs  []string
}

// NewScripted returns a backend that succeeds immediately.
func NewScripted() *Scripted {
	return &Scripted{vol: make(map[string]int)}
}

// SetDelay sets how long each call waits before the optional gate.
func (s *Scripted) SetDelay(d time.Duration) {
	s.mu.Lock()
	s.delay = d
	s.mu.Unlock()
}

// SetGate makes every call wait until ch is closed. Nil disables the gate.
func (s *Scripted) SetGate(ch chan struct{}) {
	s.mu.Lock()
	s.gate = ch
	s.mu.Unlock()
}

// SetFailCreates makes the next n create calls that finish fail, then succeed.
// A call cancelled while waiting does not consume one of those failures.
func (s *Scripted) SetFailCreates(n int) {
	s.mu.Lock()
	s.failCreates = n
	s.mu.Unlock()
}

// SetFailDeletes makes the next n delete calls that finish fail, then succeed.
func (s *Scripted) SetFailDeletes(n int) {
	s.mu.Lock()
	s.failDeletes = n
	s.mu.Unlock()
}

// Inflight is the number of calls inside the backend right now.
func (s *Scripted) Inflight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflight
}

// MaxInflight is the highest number of overlapping calls observed.
func (s *Scripted) MaxInflight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxInflight
}

// MaxVolumeInflight is the highest number of overlapping calls for one volume.
func (s *Scripted) MaxVolumeInflight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxVol
}

// CreateCalls is the number of create invocations so far, including those still waiting.
func (s *Scripted) CreateCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.creates
}

// DeleteCalls is the number of delete invocations so far, including those still waiting.
func (s *Scripted) DeleteCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deletes
}

// RequestIDs are the ids seen on each call, in start order, including calls still waiting.
// An empty id means the caller did not put one on the context.
func (s *Scripted) RequestIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.requestIDs))
	copy(out, s.requestIDs)
	return out
}

// CreateSnapshot implements StorageBackend.
func (s *Scripted) CreateSnapshot(ctx context.Context, volumeID, snapshotID string) error {
	return s.call(ctx, volumeID, true)
}

// DeleteSnapshot implements StorageBackend.
func (s *Scripted) DeleteSnapshot(ctx context.Context, volumeID, snapshotID string) error {
	return s.call(ctx, volumeID, false)
}

func (s *Scripted) call(ctx context.Context, volumeID string, create bool) error {
	s.mu.Lock()
	s.inflight++
	if s.inflight > s.maxInflight {
		s.maxInflight = s.inflight
	}
	s.vol[volumeID]++
	if s.vol[volumeID] > s.maxVol {
		s.maxVol = s.vol[volumeID]
	}
	if create {
		s.creates++
	} else {
		s.deletes++
	}
	s.requestIDs = append(s.requestIDs, logger.RequestID(ctx))
	delay := s.delay
	gate := s.gate
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.inflight--
		s.vol[volumeID]--
		s.mu.Unlock()
	}()

	if err := ctxutil.Sleep(ctx, delay); err != nil {
		return err
	}
	if gate != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-gate:
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if create {
		if s.failCreates > 0 {
			s.failCreates--
			return ErrCreateFailed
		}
		return nil
	}
	if s.failDeletes > 0 {
		s.failDeletes--
		return ErrDeleteFailed
	}
	return nil
}

var _ StorageBackend = (*Scripted)(nil)
