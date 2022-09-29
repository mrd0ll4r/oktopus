-- ==================================================================
-- CID-level Metadata
-- This is present for all filesystem-related CIDs, regardless of whether
-- we were able to download them.

-- Blocks on IPFS, identified by their multihash.
CREATE TABLE blocks
(
    id        BIGSERIAL NOT NULL PRIMARY KEY,
    multihash BYTEA     NOT NULL UNIQUE
);

-- CIDs of blocks.
-- Primarily, this covers the case when a single block is interpreted using
-- different codecs.
CREATE TABLE cids
(
    id       BIGSERIAL NOT NULL PRIMARY KEY,
    codec    BIGINT    NOT NULL,
    block_id BIGINT    NOT NULL REFERENCES blocks (id),
    UNIQUE (codec, block_id)
);

CREATE INDEX cids_block_id ON cids (block_id);

-- Lookup table for download types (block/dag).
CREATE TABLE download_types
(
    id   SERIAL NOT NULL PRIMARY KEY,
    name TEXT   NOT NULL UNIQUE
);

INSERT INTO download_types(name)
VALUES
    -- Downloading the singular block failed, i.e., _no_ information was gained.
    ('block'),
    -- Downloading the DAG referenced by the block failed.
    -- This happens for files and directories, where we try to download the
    -- DAG to hash a file, or list a directory.
    -- This is, slightly incorrectly, also used for raw blocks, in case
    -- downloading for file hashing fails (which hopefully doesn't happen if
    -- we were able to download the block earlier).
    ('dag');

-- Successful downloads of a block or corresponding DAG.
CREATE TABLE successful_downloads
(
    block_id         BIGINT                   NOT NULL REFERENCES blocks (id),
    download_type_id INT                      NOT NULL REFERENCES download_types (id),
    ts               TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (block_id, download_type_id, ts)
);

CREATE INDEX successful_downloads_block_id ON successful_downloads (block_id);
CREATE INDEX successful_downloads_block_id_download_type_id ON successful_downloads (block_id, download_type_id);

-- Failed downloads of a block or corresponding DAG.
CREATE TABLE failed_downloads
(
    block_id         BIGINT                   NOT NULL REFERENCES blocks (id),
    download_type_id INT                      NOT NULL REFERENCES download_types (id),
    ts               TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (block_id, download_type_id, ts)
);

CREATE INDEX failed_downloads_block_id ON failed_downloads (block_id);
CREATE INDEX failed_downloads_block_id_download_type_id ON failed_downloads (block_id, download_type_id);

-- ==================================================================
-- Block-level Metadata
-- This is metadata about single blocks.
-- Only present if we were able to download the block.

-- UnixFS data types.
CREATE TABLE unixfs_types
(
    id   SERIAL NOT NULL PRIMARY KEY,
    name TEXT   NOT NULL UNIQUE
);

INSERT INTO unixfs_types(name)
VALUES
    -- This encompasses both UnixFS "Raw" as well as the "raw" codec.
    -- They can be differentiated via the codec of the CID.
    ('raw'),
    -- Single-layer directories.
    ('directory'),
    -- UnixFS "File"s, which can appear both as inner nodes as well as leaves
    -- of a file DAG.
    ('file'),
    -- Metadata, ignored.
    ('metadata'),
    -- Symlinks, ignored.
    ('symlink'),
    -- Hash-array mapped tries, used for directory DAGs.
    -- These can be HAMT roots as well as inner nodes of a HAMT DAG.
    ('HAMTShard');

-- Statistics about single IPFS blocks.
-- This stores block size and function of the block within the UnixFS file
-- system.
-- This stores information for dag-pb and raw blocks, as both are part of the
-- filesystem, but we only index those blocks anyway (for now).
CREATE TABLE block_stats
(
    block_id       BIGINT NOT NULL PRIMARY KEY REFERENCES blocks (id),
    -- The size of the raw block, as it appears on the wire.
    block_size     INT    NOT NULL,
    -- The function of this block in the filesystem
    unixfs_type_id INT    NOT NULL REFERENCES unixfs_types (id)
);

-- DAG links of a block.
-- This is only populated for dag-pb blocks, since raw blocks cannot have
-- references.
CREATE TABLE block_links
(
    block_id          BIGINT NOT NULL REFERENCES blocks (id),
    -- The spec guarantees this to be UTF8 (and we need to trust that).
    name              TEXT   NOT NULL,
    -- This _should_ list the cumulative size of the referenced DAG.
    size              BIGINT NOT NULL,
    -- The referenced CID.
    referenced_cid_id BIGINT NOT NULL REFERENCES cids (id),
    PRIMARY KEY (block_id, name, referenced_cid_id)
);

-- ==================================================================
-- File-level Metadata
-- Information about file-like DAGs, if we were able to download them.
-- This is stored once, for the root of the DAG.
-- DAGs can also be single-layer, for example for raw blocks.

-- A list of MIME types, created lazily
CREATE TABLE mime_types
(
    id   SERIAL NOT NULL PRIMARY KEY,
    name TEXT   NOT NULL UNIQUE
);

-- MIME type of a UnixFS file.
-- This is only populated for blocks that are believed to be roots of file DAGs,
-- which includes raw blocks that are directory entries or were fed directly
-- into the indexer.
CREATE TABLE block_file_mime_types
(
    block_id     BIGINT NOT NULL PRIMARY KEY REFERENCES blocks (id),
    mime_type_id INT    NOT NULL REFERENCES mime_types (id)
);

-- Alternative CIDs for a UnixFS file.
-- This is only populated for blocks that are believed to be roots of file DAGs,
-- which includes raw blocks that are directory entries or were fed directly
-- into the indexer.
CREATE TABLE block_file_alternative_cids
(
    block_id BIGINT NOT NULL REFERENCES blocks (id),
    -- The binary encoded version 1 CID.
    cid_v1   BYTEA  NOT NULL,
    PRIMARY KEY (block_id, cid_v1)
);

CREATE INDEX block_file_alternative_cids_block_id ON block_file_alternative_cids (block_id);

-- A list of hash functions and their digest size.
CREATE TABLE hash_types
(
    id   SERIAL NOT NULL PRIMARY KEY,
    name TEXT   NOT NULL UNIQUE
);

INSERT INTO hash_types(name)
VALUES ('SHA2_256');

-- Hashes of files stored on IPFS.
-- This is calculated over the file as returned by `ipfs cat`, i.e., the
-- user-view of a file.
-- This is only populated for blocks that are believed to be roots of file DAGs,
-- which includes raw blocks that are directory entries or were fed directly
-- into the indexer.
CREATE TABLE block_file_hashes
(
    block_id     BIGINT NOT NULL REFERENCES blocks (id),
    hash_type_id INT    NOT NULL REFERENCES hash_types (id),
    digest       BYTEA  NOT NULL,
    PRIMARY KEY (block_id, hash_type_id)
);

CREATE INDEX block_file_hashes_block_id ON block_file_hashes (block_id);

-- ==================================================================
-- Directory-level Metadata
-- This is only populated for UnixFS directories and HAMTShards, if we were
-- able to download them.

-- Directory entries.
-- This is only populated for UnixFS "Directory" or "HAMTShard" blocks.
-- For HAMTShards, this lists entries of the entire HAMT rooted in this block.
CREATE TABLE directory_entries
(
    block_id          BIGINT NOT NULL REFERENCES blocks (id),
    name              TEXT   NOT NULL,
    size              BIGINT NOT NULL,
    referenced_cid_id BIGINT NOT NULL REFERENCES cids (id),
    PRIMARY KEY (block_id, name, referenced_cid_id)
);

CREATE INDEX directory_entries_block_id ON directory_entries (block_id);