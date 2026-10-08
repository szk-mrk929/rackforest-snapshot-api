CREATE TABLE snapshots (
    insert_seq BIGSERIAL PRIMARY KEY,
    id TEXT NOT NULL UNIQUE,
    tenant_id TEXT NOT NULL,
    volume_id TEXT NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    error TEXT NOT NULL DEFAULT '',
    idempotency_key TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT snapshots_status CHECK (
        status IN (
            'pending',
            'creating',
            'ready',
            'failed',
            'deleting',
            'deleted',
            'error_deleting'
        )
    )
);

CREATE UNIQUE INDEX snapshots_tenant_idempotency ON snapshots (tenant_id, idempotency_key)
WHERE
    idempotency_key <> '';

CREATE INDEX snapshots_tenant_list ON snapshots (tenant_id, insert_seq);
