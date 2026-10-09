-- ============================================================================
-- Distributed version of SecureShare
--
--  * File contents are no longer stored inside PostgreSQL. They are split into
--    encrypted chunks that are replicated on several storage nodes.
--  * PostgreSQL only holds METADATA (who owns what, which chunk lives where).
--  * Private keys no longer live on one API server's disk (that would break as
--    soon as a second API node exists). They are stored here, sealed with
--    AES-256-GCM under a MASTER_KEY that only the servers know.
--
-- NOTE: legacy rows in files.encrypted_file are discarded (use a fresh DB).
-- ============================================================================

ALTER TABLE files DROP COLUMN IF EXISTS encrypted_file;
ALTER TABLE files DROP COLUMN IF EXISTS iv;
ALTER TABLE files ADD COLUMN IF NOT EXISTS chunk_size  INTEGER NOT NULL DEFAULT 1048576;
ALTER TABLE files ADD COLUMN IF NOT EXISTS chunk_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE users ADD COLUMN IF NOT EXISTS private_key_enc BYTEA;

-- Ordered list of chunks that make up a file.
CREATE TABLE IF NOT EXISTS file_chunks (
    file_id  UUID    NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    idx      INTEGER NOT NULL,
    chunk_id TEXT    NOT NULL,      -- SHA-256 (hex) of the ciphertext = content address
    nonce    BYTEA   NOT NULL,      -- AES-GCM nonce used for this chunk
    size     INTEGER NOT NULL,      -- ciphertext size in bytes
    PRIMARY KEY (file_id, idx)
);
CREATE INDEX IF NOT EXISTS idx_file_chunks_chunk ON file_chunks(chunk_id);

-- Which storage nodes hold a copy of which chunk (the replica map).
CREATE TABLE IF NOT EXISTS chunk_replicas (
    chunk_id   TEXT        NOT NULL,
    node_id    VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (chunk_id, node_id)
);
CREATE INDEX IF NOT EXISTS idx_chunk_replicas_node ON chunk_replicas(node_id);

-- Small key/value table used for cluster-wide facts (e.g. who is the leader).
CREATE TABLE IF NOT EXISTS cluster_meta (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
