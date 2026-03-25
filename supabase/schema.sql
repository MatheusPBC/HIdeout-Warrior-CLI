create table if not exists artifact_catalog (
    artifact_key text primary key,
    artifact_type text not null,
    bucket_name text not null,
    object_path text not null,
    local_path text,
    content_sha256 text not null,
    size_bytes bigint not null,
    metadata jsonb not null default '{}'::jsonb,
    checksum_validated boolean not null default false,
    last_verified_at timestamptz,
    retention_expires_at timestamptz,
    deleted_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists artifact_catalog_checksum_idx
on artifact_catalog(checksum_validated)
where checksum_validated = false;

create index if not exists artifact_catalog_retention_idx
on artifact_catalog(retention_expires_at)
where retention_expires_at is not null;

create index if not exists artifact_catalog_deleted_idx
on artifact_catalog(deleted_at)
where deleted_at is not null;

create table if not exists active_models (
    family text primary key,
    active_run_id text,
    model_path text,
    model_sha256 text,
    metrics jsonb not null default '{}'::jsonb,
    payload jsonb not null default '{}'::jsonb,
    updated_at timestamptz not null default now()
);

create table if not exists snapshot_runs (
    run_id text primary key,
    snapshot_date text not null,
    bronze_rows integer not null default 0,
    silver_rows integer not null default 0,
    gold_rows integer not null default 0,
    summary jsonb not null default '{}'::jsonb,
    updated_at timestamptz not null default now()
);

create table if not exists firehose_checkpoints (
    checkpoint_name text primary key,
    next_change_id text,
    pages_processed bigint not null default 0,
    events_ingested bigint not null default 0,
    duplicates_skipped bigint not null default 0,
    updated_at timestamptz not null default now()
);

-- Tracks NDJSON landing files uploaded to Supabase Storage (Bloco 2: Raw Ingest Landing)
create table if not exists firehose_raw_manifest (
    id bigserial primary key,
    run_id text not null,
    object_path text not null,
    rows_count bigint not null default 0,
    page_start_change_id text,
    page_end_change_id text,
    file_size_bytes bigint,
    content_sha256 text,
    checksum_validated boolean not null default false,
    last_verified_at timestamptz,
    retention_expires_at timestamptz,
    uploaded_at timestamptz not null default now(),
    status text not null default 'pending' check (status in ('pending', 'uploaded', 'failed')),
    error_message text,
    created_at timestamptz not null default now()
);

-- Unique constraint for upsert by object_path (required by on_conflict="object_path")
create unique index if not exists firehose_raw_manifest_object_path_idx on firehose_raw_manifest(object_path);

-- Index for querying by run_id (batch operations)
create index if not exists firehose_raw_manifest_run_id_idx on firehose_raw_manifest(run_id);

-- Index for querying recent uploads and cleanup operations
create index if not exists firehose_raw_manifest_uploaded_at_idx on firehose_raw_manifest(uploaded_at desc);

-- Index for status-based queries (pending uploads retry, etc.)
create index if not exists firehose_raw_manifest_status_idx on firehose_raw_manifest(status) where status != 'uploaded';

create index if not exists firehose_raw_manifest_checksum_idx
on firehose_raw_manifest(checksum_validated)
where checksum_validated = false;

create index if not exists firehose_raw_manifest_retention_idx
on firehose_raw_manifest(retention_expires_at)
where retention_expires_at is not null;
