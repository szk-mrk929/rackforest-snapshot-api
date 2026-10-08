// Package postgres is the durable Store. Quota and idempotency share one
// transaction, with an advisory lock per tenant, so two API processes cannot
// both admit the snapshot that fills the quota.
package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"rackforest-snapshot-api/src/domain"
	"rackforest-snapshot-api/src/store"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Store is a PostgreSQL snapshot repository.
type Store struct {
	pool *pgxpool.Pool
}

var _ store.Store = (*Store)(nil)

// Open connects, applies migrations, and returns a store. The caller Closes it.
func Open(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("postgres pool: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the pool.
func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("migration table: %w", err)
	}
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && len(entry.Name()) > 4 {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		var applied bool
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, name).Scan(&applied); err != nil {
			return err
		}
		if applied {
			continue
		}
		body, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(body)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Create implements store.Store.
func (s *Store) Create(ctx context.Context, snap domain.Snapshot, quota int, idempotencyKey string) (domain.Snapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return domain.Snapshot{}, false, err
	}
	if err := store.ValidateNew(snap, idempotencyKey); err != nil {
		return domain.Snapshot{}, false, err
	}
	snap.CreatedAt = snap.CreatedAt.UTC()
	snap.UpdatedAt = snap.UpdatedAt.UTC()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Snapshot{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "snapshot-tenant:"+snap.TenantID); err != nil {
		return domain.Snapshot{}, false, err
	}

	if idempotencyKey != "" {
		existing, err := getOne(ctx, tx, `WHERE tenant_id = $1 AND idempotency_key = $2`, snap.TenantID, idempotencyKey)
		switch {
		case err == nil:
			if existing.VolumeID != snap.VolumeID || existing.Name != snap.Name {
				return domain.Snapshot{}, false, domain.ErrIdempotencyConflict
			}
			if err := tx.Commit(ctx); err != nil {
				return domain.Snapshot{}, false, err
			}
			return existing, false, nil
		case errors.Is(err, domain.ErrNotFound):
		default:
			return domain.Snapshot{}, false, err
		}
	}

	var used int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM snapshots
		WHERE tenant_id = $1 AND status <> 'deleted'`, snap.TenantID).Scan(&used); err != nil {
		return domain.Snapshot{}, false, err
	}
	if used >= quota {
		return domain.Snapshot{}, false, domain.ErrQuotaExceeded
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO snapshots (
			id, tenant_id, volume_id, name, status, attempts, error, idempotency_key, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		snap.ID, snap.TenantID, snap.VolumeID, snap.Name, string(snap.Status), snap.Attempts, snap.Error, idempotencyKey, snap.CreatedAt, snap.UpdatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return domain.Snapshot{}, false, fmt.Errorf("%w: duplicate snapshot id", domain.ErrInvalidArgument)
		}
		return domain.Snapshot{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Snapshot{}, false, err
	}
	return snap, true, nil
}

// Get implements store.Store.
func (s *Store) Get(ctx context.Context, tenantID, id string) (domain.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return domain.Snapshot{}, err
	}
	return getOne(ctx, s.pool, `WHERE id = $1 AND tenant_id = $2`, id, tenantID)
}

// List implements store.Store. Results follow insertion order.
func (s *Store) List(ctx context.Context, tenantID string, f store.Filter) ([]domain.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.Status != "" && !f.Status.Valid() {
		return nil, fmt.Errorf("%w: status %q", domain.ErrInvalidArgument, f.Status)
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, tenant_id, volume_id, name, status, attempts, error, created_at, updated_at
		FROM snapshots
		WHERE tenant_id = $1
		  AND ($2 = '' OR volume_id = $2)
		  AND ($3 = '' OR status = $3)
		ORDER BY insert_seq`, tenantID, f.VolumeID, string(f.Status))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collect(rows)
}

// Update implements store.Store.
func (s *Store) Update(ctx context.Context, tenantID, id string, fn func(domain.Snapshot) (domain.Snapshot, error)) (domain.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return domain.Snapshot{}, err
	}
	if fn == nil {
		return domain.Snapshot{}, fmt.Errorf("%w: nil update", domain.ErrInvalidArgument)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Snapshot{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	prev, err := getOne(ctx, tx, `WHERE id = $1 AND tenant_id = $2 FOR UPDATE`, id, tenantID)
	if err != nil {
		return domain.Snapshot{}, err
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
	next.UpdatedAt = next.UpdatedAt.UTC()
	if _, err := tx.Exec(ctx, `
		UPDATE snapshots
		SET status = $3, attempts = $4, error = $5, updated_at = $6
		WHERE id = $1 AND tenant_id = $2`,
		id, tenantID, string(next.Status), next.Attempts, next.Error, next.UpdatedAt); err != nil {
		return domain.Snapshot{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Snapshot{}, err
	}
	return next, nil
}

// RemovePending implements store.Store.
func (s *Store) RemovePending(ctx context.Context, tenantID, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM snapshots
		WHERE id = $1 AND tenant_id = $2 AND status = 'pending'`, id, tenantID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var status string
	err = s.pool.QueryRow(ctx, `SELECT status FROM snapshots WHERE id = $1 AND tenant_id = $2`, id, tenantID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: only a pending snapshot can be removed", domain.ErrInvalidState)
}

// Recoverable implements store.Store.
func (s *Store) Recoverable(ctx context.Context) ([]domain.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, tenant_id, volume_id, name, status, attempts, error, created_at, updated_at
		FROM snapshots
		WHERE status IN ('pending', 'creating', 'deleting')
		ORDER BY insert_seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collect(rows)
}

type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func getOne(ctx context.Context, q queryRower, where string, args ...any) (domain.Snapshot, error) {
	var snap domain.Snapshot
	var status string
	err := q.QueryRow(ctx, `
		SELECT id, tenant_id, volume_id, name, status, attempts, error, created_at, updated_at
		FROM snapshots `+where, args...).Scan(
		&snap.ID, &snap.TenantID, &snap.VolumeID, &snap.Name, &status, &snap.Attempts, &snap.Error, &snap.CreatedAt, &snap.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Snapshot{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Snapshot{}, err
	}
	snap.Status = domain.Status(status)
	snap.CreatedAt = snap.CreatedAt.UTC()
	snap.UpdatedAt = snap.UpdatedAt.UTC()
	return snap, nil
}

func collect(rows pgx.Rows) ([]domain.Snapshot, error) {
	out := make([]domain.Snapshot, 0)
	for rows.Next() {
		var snap domain.Snapshot
		var status string
		var created, updated time.Time
		if err := rows.Scan(&snap.ID, &snap.TenantID, &snap.VolumeID, &snap.Name, &status, &snap.Attempts, &snap.Error, &created, &updated); err != nil {
			return nil, err
		}
		snap.Status = domain.Status(status)
		snap.CreatedAt = created.UTC()
		snap.UpdatedAt = updated.UTC()
		out = append(out, snap)
	}
	return out, rows.Err()
}
