-- Add some more info to the hash_types table
ALTER TABLE hash_types
    ADD multiformat_code BIGINT;
UPDATE hash_types
SET multiformat_code=x'12'::BIGINT
WHERE name = 'SHA2_256';
ALTER TABLE hash_types
    ALTER COLUMN multiformat_code SET NOT NULL;

ALTER TABLE hash_types
    ADD digest_bytes INTEGER;
UPDATE hash_types
SET digest_bytes = 32
WHERE name = 'SHA2_256';
ALTER TABLE hash_types
    ALTER COLUMN digest_bytes SET NOT NULL;

-- Add the hash functions and sizes for which we generate CIDs
-- The naming is the same as used by kubo.
-- The numbers in the names sometimes refer to the digest size, sometimes to a
-- function parameter.
INSERT INTO hash_types(name, multiformat_code, digest_bytes)
VALUES ('SHA1', x'11'::BIGINT, 20),
       ('SHA2_512', x'13'::BIGINT, 64),
       ('SHA3_224', x'17'::BIGINT, 28),
       ('SHA3_256', x'16'::BIGINT, 32),
       ('SHA3_384', x'15'::BIGINT, 48),
       ('SHA3_512', x'14'::BIGINT, 64),
       ('DBL_SHA2_256', x'56'::BIGINT, 32),
       ('KECCAK_256', x'1b'::BIGINT, 32),
       ('KECCAK_512', x'1d'::BIGINT, 64),
       ('BLAKE3_256', x'1e'::BIGINT, 32),
       ('SHAKE_256', x'19'::BIGINT, 64);

-- =========================
-- Split index of block_file_alternative_cids

-- Extract codec
ALTER TABLE block_file_alternative_cids
    ADD codec BIGINT;
UPDATE block_file_alternative_cids c
SET codec = get_byte(c.cid_v1, 1);
ALTER TABLE block_file_alternative_cids
    ALTER COLUMN codec SET NOT NULL;

-- Extract hash function (and size)
ALTER TABLE block_file_alternative_cids
    ADD hash_type_id INT REFERENCES hash_types (id);
UPDATE block_file_alternative_cids c
SET hash_type_id = (SELECT id
                    FROM hash_types t
                    WHERE t.multiformat_code = get_byte(c.cid_v1, 2)
                      AND t.digest_bytes = get_byte(c.cid_v1, 3));
ALTER TABLE block_file_alternative_cids
    ALTER COLUMN hash_type_id SET NOT NULL;

-- This contains duplicates.
-- We figured out very late that `ipfs cat` with a timeout, via the HTTP API and the library we used,
-- can potentially truncate file data. That's not great.
-- That, in turn, causes different alternative CIDs to be generated.
-- We can, however, use those different alternative CIDs for the same block and hash function to identify blocks where
-- this has happened, _if_ we downloaded them more than once.
-- This will also remove potentially correct file metadata from the database.
-- The best thing we can do here is identify cases where it happened at least once.
-- There will still be incomplete file data in the DB after this...

CREATE TEMP TABLE potentially_incomplete_file_downloads
(
    block_id BIGINT PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO potentially_incomplete_file_downloads (block_id)
SELECT DISTINCT c1.block_id
FROM block_file_alternative_cids c1,
     block_file_alternative_cids c2
WHERE c1.block_id = c2.block_id
  AND c1.codec = c2.codec
  AND c1.hash_type_id = c2.hash_type_id
  AND c1.cid_v1 != c2.cid_v1;

DO
$$
    DECLARE
        num_mismatching_files BIGINT := 0;
    BEGIN
        num_mismatching_files := COUNT(*) FROM potentially_incomplete_file_downloads;
        RAISE NOTICE 'Found % potentially incomplete file records, will remove', num_mismatching_files;
    END;
$$
LANGUAGE plpgsql;

DELETE
FROM block_file_alternative_cids
WHERE block_id IN (SELECT block_id FROM potentially_incomplete_file_downloads);
DELETE
FROM successful_downloads
WHERE block_id IN (SELECT block_id FROM potentially_incomplete_file_downloads)
  AND download_type_id = 2;
DELETE
FROM block_file_hashes
WHERE block_id IN (SELECT block_id FROM potentially_incomplete_file_downloads);
DELETE
FROM block_file_metadata
WHERE block_id IN (SELECT block_id FROM potentially_incomplete_file_downloads);

-- Add unique index on (block_id, codec, hash_type_id)
CREATE UNIQUE INDEX block_file_alternative_cids_block_id_codec_hash_type_id ON block_file_alternative_cids (block_id, codec, hash_type_id);

-- Remove old primary key
ALTER TABLE block_file_alternative_cids
    DROP CONSTRAINT block_file_alternative_cids_pkey;

-- Rename our index
ALTER INDEX block_file_alternative_cids_block_id_codec_hash_type_id RENAME TO block_file_alternative_cids_pkey;

-- Use new index as primary key
ALTER TABLE block_file_alternative_cids
    ADD PRIMARY KEY USING INDEX block_file_alternative_cids_pkey;

-- Truncate CIDs to drop the first four bytes: version, codec, hash type, digest length
-- This uses 1-based indexing, for some reason.
UPDATE block_file_alternative_cids c
SET cid_v1 = substring(c.cid_v1 FROM 5);

-- Rename CID column to reflect what it really is
ALTER TABLE block_file_alternative_cids
    RENAME COLUMN cid_v1 TO digest;
