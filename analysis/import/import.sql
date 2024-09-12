BEGIN;

-- Helper function to print useful information
CREATE
OR REPLACE FUNCTION print_notice(msg TEXT) RETURNS void AS
$$
BEGIN
    RAISE
NOTICE '%', msg;
END;
$$
LANGUAGE plpgsql;

-- ===============================================
-- Add new MIME types, if any.
SELECT print_notice('Importing MIME types...');
INSERT INTO analysis.mime_types (name)
SELECT name
FROM public.mime_types ON CONFLICT DO NOTHING;

-- Create temp table for MIME type ID mapping
CREATE
TEMP TABLE mime_type_id_mapping
(
    public_mime_type_id   INT NOT NULL UNIQUE ,
    analysis_mime_type_id INT NOT NULL UNIQUE
)
    ON COMMIT DROP;

INSERT INTO mime_type_id_mapping (public_mime_type_id, analysis_mime_type_id)
SELECT pm.id, am.id
FROM public.mime_types pm,
     analysis.mime_types am
WHERE pm.name = am.name;

-- ===============================================
-- Import new blocks.
SELECT print_notice('Importing blocks...');
INSERT INTO analysis.blocks (multihash)
SELECT multihash
FROM public.blocks ON CONFLICT DO NOTHING;

-- Create temp table for block ID mapping
CREATE
TEMP TABLE block_id_mapping
(
    public_block_id   BIGINT NOT NULL UNIQUE,
    analysis_block_id BIGINT NOT NULL UNIQUE
)
    ON COMMIT DROP;

INSERT INTO block_id_mapping (public_block_id, analysis_block_id)
SELECT pb.id, ab.id
FROM public.blocks pb,
     analysis.blocks ab
WHERE pb.multihash = ab.multihash;

-- ===============================================
-- Create temp table to record potentially incomplete file downloads.
-- Apparently, we sometimes record a successful download for a file, but end up with a partial download.
-- I think that happens if there's a race in kubo between the context timeout and having started to send data.
-- Anyway. That's not great, so we check if any newly-imported data has a smaller file size.
-- The file size recorded is the size of the vector of bytes we operate on, so that's safe at least.
-- If we see a smaller size (or NULL, from an older schema), we exclude that record from being imported.
-- Conversely, if we see a larger size (or our size is NULL), we upgrade our records accordingly.
SELECT print_notice('Checking for potentially incomplete file downloads...');
CREATE
TEMP TABLE block_file_downgrades
(
    public_block_id   BIGINT NOT NULL,
    analysis_block_id BIGINT NOT NULL
)
    ON COMMIT DROP;
INSERT INTO block_file_downgrades (public_block_id, analysis_block_id)
SELECT pm.block_id, am.block_id
FROM public.block_file_metadata pm,
     block_id_mapping bim,
     analysis.block_file_metadata am
WHERE pm.block_id = bim.public_block_id
  AND am.block_id = bim.analysis_block_id
  AND (pm.file_size IS NULL OR pm.file_size < am.file_size);

CREATE
TEMP TABLE block_file_upgrades
(
    public_block_id   BIGINT NOT NULL,
    analysis_block_id BIGINT NOT NULL
)
    ON COMMIT DROP;
INSERT INTO block_file_upgrades (public_block_id, analysis_block_id)
SELECT pm.block_id, am.block_id
FROM public.block_file_metadata pm,
     block_id_mapping bim,
     analysis.block_file_metadata am
WHERE pm.block_id = bim.public_block_id
  AND am.block_id = bim.analysis_block_id
  AND (am.file_size IS NULL OR pm.file_size > am.file_size);

DO
language plpgsql
$$
    DECLARE
num_analysis_file_metadata BIGINT := 0;
        num_public_file_metadata
BIGINT := 0;
        num_overlap_file_metadata
BIGINT := 0;
        num_new_file_metadata
BIGINT := 0;
        num_file_metadata_downgrades
BIGINT := 0;
        num_file_metadata_upgrades
BIGINT := 0;
BEGIN
        num_analysis_file_metadata
:= COUNT(*) FROM analysis.block_file_metadata;
        num_public_file_metadata
:= COUNT(*) FROM public.block_file_metadata;
        num_overlap_file_metadata
:= COUNT(*)
                                     FROM public.block_file_metadata pm,
                                          block_id_mapping bim,
                                          analysis.block_file_metadata am
                                     WHERE pm.block_id = bim.public_block_id
                                       AND am.block_id = bim.analysis_block_id;
        num_new_file_metadata
:=
                    (SELECT COUNT(*) FROM public.block_file_metadata) -
                    (SELECT COUNT(*)
                     FROM public.block_file_metadata pm,
                          block_id_mapping bim,
                          analysis.block_file_metadata am
                     WHERE pm.block_id = bim.public_block_id
                       AND am.block_id = bim.analysis_block_id);
        num_file_metadata_downgrades
:=
                COUNT(*)
                FROM block_file_downgrades;
        num_file_metadata_upgrades
:=                COUNT(*)
                FROM block_file_upgrades;
        RAISE
NOTICE 'Total number of file metadata records in analysis database: %', num_analysis_file_metadata;
        RAISE
NOTICE 'Total number of file metadata records in import database: %', num_public_file_metadata;
        RAISE
NOTICE 'Total number of new file metadata records to be imported: %', num_new_file_metadata;
        RAISE
NOTICE 'Total number of overlapping file metadata records: %', num_overlap_file_metadata;
        RAISE
NOTICE '...of which are downgrades, which will be skipped: %', num_file_metadata_downgrades;
        RAISE
NOTICE '...of which are upgrades, which will replace existing data: %', num_file_metadata_upgrades;
END
$$;

-- Remove data to be upgraded from the import database from the analysis database.
-- These are files that are probably incomplete in the analysis database.
DELETE
FROM analysis.block_file_metadata
WHERE block_id IN (SELECT analysis_block_id FROM block_file_upgrades);
DELETE
FROM analysis.block_file_hashes
WHERE block_id IN (SELECT analysis_block_id FROM block_file_upgrades);
DELETE
FROM analysis.block_file_alternative_cids
WHERE block_id IN (SELECT analysis_block_id FROM block_file_upgrades);
DELETE
FROM analysis.successful_downloads
WHERE download_type_id = 2
  AND block_id IN (SELECT analysis_block_id FROM block_file_upgrades);

-- Remove potential downgrades from import database.
-- These are files that are probably incomplete in the import database.
DELETE
FROM public.block_file_metadata
WHERE block_id IN (SELECT public_block_id FROM block_file_downgrades);
DELETE
FROM public.block_file_hashes
WHERE block_id IN (SELECT public_block_id FROM block_file_downgrades);
DELETE
FROM public.block_file_alternative_cids
WHERE block_id IN (SELECT public_block_id FROM block_file_downgrades);
DELETE
FROM public.successful_downloads
WHERE download_type_id = 2
  AND block_id IN (SELECT public_block_id FROM block_file_downgrades);


-- ===============================================
-- Import successful downloads.
-- There should be no conflicts, if the exports are from different runs.
SELECT print_notice('Importing successful downloads...');
INSERT INTO analysis.successful_downloads (block_id, download_type_id, end_ts, start_ts, head_finished_ts,
                                           download_finished_ts)
SELECT bim.analysis_block_id,
       psd.download_type_id,
       psd.end_ts,
       psd.start_ts,
       psd.head_finished_ts,
       psd.download_finished_ts
FROM public.successful_downloads psd,
     block_id_mapping bim
WHERE psd.block_id = bim.public_block_id;

-- ===============================================
-- Import failed downloads.
-- Here, too, we shouldn't find conflicts.
SELECT print_notice
           ('Importing failed downloads...');
INSERT INTO analysis.failed_downloads (block_id, download_type_id, ts)
SELECT bim.analysis_block_id, pfd.download_type_id, pfd.ts
FROM public.failed_downloads pfd,
     block_id_mapping bim
WHERE pfd.block_id = bim.public_block_id;

-- ===============================================
-- Import CIDs.
SELECT print_notice
           ('Importing CIDs...');
INSERT INTO analysis.cids (codec, block_id)
SELECT pc.codec, bim.analysis_block_id
FROM public.cids pc,
     block_id_mapping bim
WHERE pc.block_id = bim.public_block_id ON CONFLICT DO NOTHING;

-- Create temp table for CID ID mapping
CREATE
TEMP TABLE cid_id_mapping
(
    public_cid_id   BIGINT NOT NULL,
    analysis_cid_id BIGINT NOT NULL
)
    ON COMMIT DROP;

INSERT INTO cid_id_mapping
SELECT pc.id, ac.id
FROM public.cids pc,
     analysis.cids ac,
     block_id_mapping bim
WHERE pc.block_id = bim.public_block_id
  AND ac.block_id = bim.analysis_block_id
  AND pc.codec = ac.codec;

-- ===============================================
-- Import block stats
-- There is an UPDATE trigger to prevent importing mismatching data for the same
-- block.
SELECT print_notice
           ('Importing block stats...');
INSERT INTO analysis.block_stats (block_id, block_size, unixfs_type_id)
SELECT bim.analysis_block_id, ps.block_size, ps.unixfs_type_id
FROM public.block_stats ps,
     block_id_mapping bim
WHERE ps.block_id = bim.public_block_id ON CONFLICT (block_id) DO
UPDATE SET block_size = excluded.block_size,
    unixfs_type_id = excluded.unixfs_type_id;

-- ===============================================
-- Import block links
-- There is an UPDATE trigger to prevent importing mismatching data for the same
-- link.
SELECT print_notice
           ('Importing block links...');
INSERT INTO analysis.block_links (block_id, name, size, referenced_cid_id)
SELECT bim.analysis_block_id, pl.name, pl.size, cim.analysis_cid_id
FROM public.block_links pl,
     block_id_mapping bim,
     cid_id_mapping cim
WHERE pl.block_id = bim.public_block_id
  AND pl.referenced_cid_id = cim.public_cid_id ON CONFLICT (block_id,name,referenced_cid_id) DO
UPDATE SET size = excluded.size;

-- ===============================================
-- Import block file metadata
-- There is an UPDATE trigger to prevent importing mismatching data for the same
-- block. The trigger also handles correct upgrades to libmime_mime_type_id.
SELECT print_notice
           ('Importing block file metadata...');
INSERT INTO analysis.block_file_metadata (block_id, freedesktop_mime_type_id,
                                          file_size, libmagic_mime_type_id)
SELECT bim.analysis_block_id,
       mim_freedesktop.analysis_mime_type_id,
       pm.file_size,
       mim_libmagic.analysis_mime_type_id
FROM public.block_file_metadata pm,
     block_id_mapping bim,
     mime_type_id_mapping mim_libmagic,
     mime_type_id_mapping mim_freedesktop
WHERE pm.block_id = bim.public_block_id
  AND pm.libmagic_mime_type_id = mim_libmagic.public_mime_type_id
  AND pm.freedesktop_mime_type_id = mim_freedesktop.public_mime_type_id ON CONFLICT (block_id) DO
UPDATE SET freedesktop_mime_type_id = excluded.freedesktop_mime_type_id,
    libmagic_mime_type_id = excluded.libmagic_mime_type_id,
    file_size = excluded.file_size;


-- ===============================================
-- Import block file hashes
-- There is an UPDATE trigger to prevent importing mismatching data for the same
-- block.
SELECT print_notice
           ('Importing block file hashes...');
INSERT INTO analysis.block_file_hashes (block_id, hash_type_id, digest)
SELECT bim.analysis_block_id, ph.hash_type_id, ph.digest
FROM public.block_file_hashes ph,
     block_id_mapping bim
WHERE ph.block_id = bim.public_block_id ON CONFLICT (block_id,hash_type_id) DO
UPDATE SET digest = excluded.digest;

-- ===============================================
-- Import block file alternative CIDs
-- Apparently these are nondeterministic, so we just ignore conflicts.
SELECT print_notice
           ('Importing block file alternative CIDs...');
INSERT INTO analysis.block_file_alternative_cids (block_id, digest, codec, hash_type_id)
SELECT bim.analysis_block_id, pa.digest, pa.codec, pa.hash_type_id
FROM public.block_file_alternative_cids pa,
     block_id_mapping bim
WHERE pa.block_id = bim.public_block_id ON CONFLICT DO NOTHING;

-- ===============================================
-- Import directory entries
-- There is an UPDATE trigger to prevent importing mismatching data for the same
-- entry.
SELECT print_notice
           ('Importing directory entries...');
INSERT INTO analysis.directory_entries (block_id, name, size, referenced_cid_id)
SELECT bim.analysis_block_id, pe.name, pe.size, cim.analysis_cid_id
FROM public.directory_entries pe,
     block_id_mapping bim,
     cid_id_mapping cim
WHERE pe.block_id = bim.public_block_id
  AND pe.referenced_cid_id = cim.public_cid_id ON CONFLICT (block_id,name,referenced_cid_id) DO
UPDATE SET size = excluded.size;

COMMIT;