package backend

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

type objectKey struct {
	volumeID   string
	snapshotID string
}

// MockConfig controls the simulated storage.
type MockConfig struct {
	MinDelay  time.Duration
	MaxDelay  time.Duration
	ErrorRate float64
	// Seed selects the random source. Zero uses the current time.
	Seed int64
}

// Mock sleeps for a configured interval and then sometimes fails.
// A create that succeeded is remembered, so a later create of the same
// snapshot returns success without rolling the error rate again.
type Mock struct {
	minDelay  time.Duration
	maxDelay  time.Duration
	errorRate float64

	mu   sync.Mutex
	rng  *rand.Rand
	have map[objectKey]struct{}
}

// NewMock returns a backend that sleeps and fails according to cfg.
func NewMock(cfg MockConfig) *Mock {
	seed := cfg.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return &Mock{
		minDelay:  cfg.MinDelay,
		maxDelay:  cfg.MaxDelay,
		errorRate: cfg.ErrorRate,
		rng:       rand.New(rand.NewSource(seed)),
		have:      make(map[objectKey]struct{}),
	}
}

// CreateSnapshot implements StorageBackend.
func (m *Mock) CreateSnapshot(ctx context.Context, volumeID, snapshotID string) error {
	if err := wait(ctx, m.nextDelay()); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	key := objectKey{volumeID: volumeID, snapshotID: snapshotID}
	if _, ok := m.have[key]; ok {
		return nil
	}
	if m.failLocked() {
		return ErrCreateFailed
	}
	m.have[key] = struct{}{}
	return nil
}

// DeleteSnapshot implements StorageBackend.
// A missing snapshot is not an error: a failed create may never have landed.
// The error rate can still fail the call, which is a transient storage error.
func (m *Mock) DeleteSnapshot(ctx context.Context, volumeID, snapshotID string) error {
	if err := wait(ctx, m.nextDelay()); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.failLocked() {
		return ErrDeleteFailed
	}
	delete(m.have, objectKey{volumeID: volumeID, snapshotID: snapshotID})
	return nil
}

func (m *Mock) nextDelay() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.maxDelay <= m.minDelay {
		return m.minDelay
	}
	span := int64(m.maxDelay - m.minDelay)
	return m.minDelay + time.Duration(m.rng.Int63n(span))
}

func (m *Mock) failLocked() bool {
	if m.errorRate <= 0 {
		return false
	}
	if m.errorRate >= 1 {
		return true
	}
	return m.rng.Float64() < m.errorRate
}

var _ StorageBackend = (*Mock)(nil)
