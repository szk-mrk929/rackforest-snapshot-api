package store

import (
	"context"
	"fmt"
	"sync"

	"rackforest-snapshot-api/src/domain"
)

type idemRef struct {
	tenant string
	key    string
}

// Memory is a process-local Store.
// mu is the transactional boundary: quota, idempotency, and status changes
// cannot interleave.
type Memory struct {
	mu    sync.Mutex
	snaps map[string]domain.Snapshot
	order []string
	idem  map[idemRef]string
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{
		snaps: make(map[string]domain.Snapshot),
		idem:  make(map[idemRef]string),
	}
}

// Create implements Store.
func (m *Memory) Create(ctx context.Context, snap domain.Snapshot, quota int, idempotencyKey string) (domain.Snapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return domain.Snapshot{}, false, err
	}
	if err := validateNew(snap, idempotencyKey); err != nil {
		return domain.Snapshot{}, false, err
	}
	snap.CreatedAt = snap.CreatedAt.UTC()
	snap.UpdatedAt = snap.UpdatedAt.UTC()

	m.mu.Lock()
	defer m.mu.Unlock()

	if idempotencyKey != "" {
		if id, ok := m.idem[idemRef{tenant: snap.TenantID, key: idempotencyKey}]; ok {
			existing := m.snaps[id]
			if existing.VolumeID != snap.VolumeID || existing.Name != snap.Name {
				return domain.Snapshot{}, false, domain.ErrIdempotencyConflict
			}
			return existing, false, nil
		}
	}

	if m.activeCount(snap.TenantID) >= quota {
		return domain.Snapshot{}, false, domain.ErrQuotaExceeded
	}
	if _, exists := m.snaps[snap.ID]; exists {
		return domain.Snapshot{}, false, fmt.Errorf("%w: duplicate snapshot id", domain.ErrInvalidArgument)
	}

	m.snaps[snap.ID] = snap
	m.order = append(m.order, snap.ID)
	if idempotencyKey != "" {
		m.idem[idemRef{tenant: snap.TenantID, key: idempotencyKey}] = snap.ID
	}
	return snap, true, nil
}

// Get implements Store.
func (m *Memory) Get(ctx context.Context, tenantID, id string) (domain.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return domain.Snapshot{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	snap, ok := m.snaps[id]
	if !ok || snap.TenantID != tenantID {
		return domain.Snapshot{}, domain.ErrNotFound
	}
	return snap, nil
}

// List implements Store. Results follow insertion order.
func (m *Memory) List(ctx context.Context, tenantID string, f Filter) ([]domain.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.Status != "" && !f.Status.Valid() {
		return nil, fmt.Errorf("%w: status %q", domain.ErrInvalidArgument, f.Status)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]domain.Snapshot, 0)
	for _, id := range m.order {
		snap := m.snaps[id]
		if snap.TenantID != tenantID {
			continue
		}
		if f.VolumeID != "" && snap.VolumeID != f.VolumeID {
			continue
		}
		if f.Status != "" && snap.Status != f.Status {
			continue
		}
		out = append(out, snap)
	}
	return out, nil
}

// Update implements Store.
func (m *Memory) Update(ctx context.Context, tenantID, id string, fn func(domain.Snapshot) (domain.Snapshot, error)) (domain.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return domain.Snapshot{}, err
	}
	if fn == nil {
		return domain.Snapshot{}, fmt.Errorf("%w: nil update", domain.ErrInvalidArgument)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	prev, ok := m.snaps[id]
	if !ok || prev.TenantID != tenantID {
		return domain.Snapshot{}, domain.ErrNotFound
	}
	next, err := fn(prev)
	if err != nil {
		return domain.Snapshot{}, err
	}
	if next.Status != prev.Status && !domain.CanTransition(prev.Status, next.Status) {
		return domain.Snapshot{}, &domain.TransitionError{From: prev.Status, To: next.Status}
	}
	next.ID = prev.ID
	next.TenantID = prev.TenantID
	next.VolumeID = prev.VolumeID
	next.Name = prev.Name
	next.CreatedAt = prev.CreatedAt
	m.snaps[id] = next
	return next, nil
}

// RemovePending implements Store.
func (m *Memory) RemovePending(ctx context.Context, tenantID, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	snap, ok := m.snaps[id]
	if !ok || snap.TenantID != tenantID {
		return domain.ErrNotFound
	}
	if snap.Status != domain.StatusPending {
		return fmt.Errorf("%w: only a pending snapshot can be removed", domain.ErrInvalidState)
	}
	delete(m.snaps, id)
	order := m.order[:0]
	for _, existing := range m.order {
		if existing != id {
			order = append(order, existing)
		}
	}
	m.order = order
	for ref, snapID := range m.idem {
		if ref.tenant == tenantID && snapID == id {
			delete(m.idem, ref)
		}
	}
	return nil
}

func (m *Memory) activeCount(tenantID string) int {
	n := 0
	for _, snap := range m.snaps {
		if snap.TenantID == tenantID && snap.Status.CountsTowardQuota() {
			n++
		}
	}
	return n
}

func validateNew(snap domain.Snapshot, idempotencyKey string) error {
	if err := domain.ValidateScopeID("snapshot_id", snap.ID); err != nil {
		return err
	}
	if err := domain.ValidateScopeID("tenant_id", snap.TenantID); err != nil {
		return err
	}
	if err := domain.ValidateScopeID("volume_id", snap.VolumeID); err != nil {
		return err
	}
	if err := domain.ValidateName(snap.Name); err != nil {
		return err
	}
	if snap.Status != domain.StatusPending {
		return fmt.Errorf("%w: new snapshot must be pending", domain.ErrInvalidArgument)
	}
	if idempotencyKey != "" {
		if err := domain.ValidateIdempotencyKey(idempotencyKey); err != nil {
			return err
		}
	}
	return nil
}

var _ Store = (*Memory)(nil)
