CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ingestion_batches (
    id UUID PRIMARY KEY,
    source TEXT NOT NULL CHECK (length(source) BETWEEN 1 AND 128),
    total_records INTEGER NOT NULL CHECK (total_records >= 0),
    indexed_records INTEGER NOT NULL DEFAULT 0 CHECK (indexed_records >= 0),
    failed_records INTEGER NOT NULL DEFAULT 0 CHECK (failed_records >= 0),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'partial', 'completed', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (indexed_records + failed_records <= total_records)
);

CREATE TABLE IF NOT EXISTS index_jobs (
    id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL REFERENCES ingestion_batches(id) ON DELETE CASCADE,
    document_id TEXT NOT NULL CHECK (length(document_id) BETWEEN 1 AND 256),
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'indexed', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (batch_id, document_id)
);

CREATE INDEX IF NOT EXISTS index_jobs_status_available_idx
    ON index_jobs (status, available_at);

CREATE INDEX IF NOT EXISTS index_jobs_batch_id_idx
    ON index_jobs (batch_id);
