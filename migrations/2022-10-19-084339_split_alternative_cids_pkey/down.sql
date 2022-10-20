-- =========================
-- block_file_alternative_cids

-- Rename digest column to represent CIDs again
ALTER TABLE block_file_alternative_cids
    RENAME COLUMN digest TO cid_v1;

-- We need this helper function to convert BIGINT to BYTEA big-endian.
-- We actually want a varint representation for the CID, but we can use this because all our values fit into seven bits, i.e. one varint byte.
-- Source: https://stackoverflow.com/questions/37248518/sql-function-to-convert-numeric-to-bytea-and-bytea-to-numeric
CREATE
OR REPLACE FUNCTION bigint2bytea(_n BIGINT) RETURNS BYTEA AS
$$
DECLARE
_b BYTEA := '\x';
    _v INTEGER;
BEGIN
    WHILE
_n > 0
        LOOP
            _v := _n % 256;
_b := SET_BYTE(('\x00' || _b), 0, _v);
_n := (_n - _v) / 256;
END LOOP;
    RETURN _b;
END;
$$
LANGUAGE PLPGSQL IMMUTABLE
                    STRICT;

-- Re-unite columns of block_file_alternative_cids into cidv1
UPDATE block_file_alternative_cids c
SET cid_v1 = ('\x01'::BYTEA || bigint2bytea(c.codec) ||
              bigint2bytea((SELECT h.multiformat_code
                            FROM hash_types h
                            WHERE h.id = c.hash_type_id)) ||
    bigint2bytea((SELECT h.digest_bytes
                            FROM hash_types h
                            WHERE h.id = c.hash_type_id)) ||
              c.cid_v1);

-- Drop the helper function
DROP FUNCTION bigint2bytea(_n BIGINT);

-- Remove old primary key
ALTER TABLE block_file_alternative_cids
DROP
CONSTRAINT block_file_alternative_cids_pkey;

-- Add unique index on (block_id, cid_v1)
CREATE UNIQUE INDEX block_file_alternative_cids_block_id_cid_v1 ON block_file_alternative_cids (block_id, cid_v1);

-- Use new index as primary key
ALTER TABLE block_file_alternative_cids
    ADD PRIMARY KEY USING INDEX block_file_alternative_cids_block_id_cid_v1;

-- Rename our index
ALTER
INDEX block_file_alternative_cids_block_id_cid_v1 RENAME TO block_file_alternative_cids_pkey;

-- Drop hash function column
ALTER TABLE block_file_alternative_cids
DROP
hash_type_id;

-- Drop codec column
ALTER TABLE block_file_alternative_cids
DROP
codec;

-- =========================
-- hash_types

-- Remove newly-added hash types
DELETE
FROM hash_types
WHERE id >= 2
  AND id <= 12;

-- Remove added columns from hash_types
ALTER TABLE hash_types
DROP
multiformat_code;
ALTER TABLE hash_types
DROP
digest_bytes;
