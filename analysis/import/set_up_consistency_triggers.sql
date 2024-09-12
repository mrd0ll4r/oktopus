-- ============================================
CREATE OR REPLACE FUNCTION analysis.check_block_stats_import_update() RETURNS trigger AS
$check$
BEGIN
    -- Check that the (supposedly immutable) data matches
    IF NEW.block_size IS DISTINCT FROM OLD.block_size THEN
        RAISE EXCEPTION 'block_size mismatch during import for analysis.block_id %: existing entry has %, new entry has %', OLD.block_id, OLD.block_size, NEW.block_size;
    END IF;
    IF NEW.unixfs_type_id IS DISTINCT FROM OLD.unixfs_type_id THEN
        RAISE EXCEPTION 'unixfs_type_id mismatch during import for analysis.block_id %: existing entry has %, new entry has %', OLD.block_id, OLD.unixfs_type_id, NEW.unixfs_type_id;
    END IF;

    RETURN NEW;
END;
$check$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER analysis_block_stats_import_equality
    BEFORE UPDATE
    ON analysis.block_stats
    FOR EACH ROW
    WHEN (OLD.block_size IS DISTINCT FROM NEW.block_size OR
          OLD.unixfs_type_id IS DISTINCT FROM NEW.unixfs_type_id)
EXECUTE FUNCTION analysis.check_block_stats_import_update();

-- ============================================
CREATE OR REPLACE FUNCTION analysis.check_block_links_import_update() RETURNS trigger AS
$check$
BEGIN
    -- Check that the (supposedly immutable) data matches
    IF NEW.size IS DISTINCT FROM OLD.size THEN
        RAISE EXCEPTION 'size mismatch during import for analysis.block_id %: existing entry has %, new entry has %', OLD.block_id, OLD.size, NEW.size;
    END IF;

    RETURN NEW;
END;
$check$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER analysis_block_links_import_equality
    BEFORE UPDATE
    ON analysis.block_links
    FOR EACH ROW
    WHEN (OLD.size IS DISTINCT FROM NEW.size)
EXECUTE FUNCTION analysis.check_block_links_import_update();

-- ============================================
CREATE OR REPLACE FUNCTION analysis.check_block_file_hashes_import_update() RETURNS trigger AS
$check$
BEGIN
    -- Check that the (supposedly immutable) data matches
    IF NEW.digest IS DISTINCT FROM OLD.digest THEN
        RAISE EXCEPTION 'digest mismatch during import for analysis.block_id %: existing entry has %, new entry has %', OLD.block_id, OLD.digest, NEW.digest;
    END IF;

    RETURN NEW;
END;
$check$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER analysis_block_file_hashes_import_equality
    BEFORE UPDATE
    ON analysis.block_file_hashes
    FOR EACH ROW
    WHEN (OLD.digest IS DISTINCT FROM NEW.digest)
EXECUTE FUNCTION analysis.check_block_file_hashes_import_update();

-- ============================================
CREATE OR REPLACE FUNCTION analysis.check_block_file_metadata_import_update() RETURNS trigger AS
$check$
BEGIN
    -- Check that the (supposedly immutable) data matches
    IF NEW.file_size IS DISTINCT FROM OLD.file_size THEN
        IF OLD.file_size IS NULL THEN
            -- Okay, we get a file size for free, keep it.
        ELSEIF NEW.file_size IS NULL THEN
            -- Keep the old one.
            NEW.file_size = old.file_size;
        ELSE
            -- They're both set, but differ. This should never happen.
            RAISE EXCEPTION 'file_size mismatch during import for analysis.block_id %: existing entry has %, new entry has %', OLD.block_id, OLD.file_size, NEW.file_size;
        end if;
    END IF;
    IF NEW.freedesktop_mime_type_id IS DISTINCT FROM OLD.freedesktop_mime_type_id THEN
        RAISE WARNING 'freedesktop_mime_type_id mismatch during import for analysis.block_id %: existing entry has %, new entry has %', OLD.block_id, OLD.freedesktop_mime_type_id, NEW.freedesktop_mime_type_id;
        NEW.freedesktop_mime_type_id = OLD.freedesktop_mime_type_id;
    END IF;

    -- Check for potential upgrade to the libmagic MIME type.
    IF OLD.libmagic_mime_type_id IS DISTINCT FROM NEW.libmagic_mime_type_id THEN
        IF NEW.libmagic_mime_type_id = (SELECT id
                                       FROM analysis.mime_types
                                       WHERE name = 'unknown/pre-libmagic-schema') THEN
            -- We had a proper MIME type, the new record doesn't, so we ignore the update.
            NEW.libmagic_mime_type_id = OLD.libmagic_mime_type_id;
        ELSEIF OLD.libmagic_mime_type_id = (SELECT id
                                           FROM analysis.mime_types
                                           WHERE name = 'unknown/pre-libmagic-schema') THEN
            -- We didn't have a proper MIME type, but now we do. Good.
        ELSE
            -- Both are proper MIME types, but they differ. This is an error.
            RAISE WARNING 'libmagic_mime_type_id mismatch and both are real MIME types during import for analysis.block_id %: existing entry has %, new entry has %', OLD.block_id, OLD.libmagic_mime_type_id, NEW.libmagic_mime_type_id;
            NEW.libmagic_mime_type_id = OLD.libmagic_mime_type_id;
        end if;
    end if;

    RETURN NEW;
END;
$check$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER analysis_block_file_metadata_import_equality
    BEFORE UPDATE
    ON analysis.block_file_metadata
    FOR EACH ROW
    WHEN (OLD.freedesktop_mime_type_id IS DISTINCT FROM NEW.freedesktop_mime_type_id
        OR OLD.file_size IS DISTINCT FROM NEW.file_size
        OR OLD.libmagic_mime_type_id IS DISTINCT FROM NEW.libmagic_mime_type_id)
EXECUTE FUNCTION analysis.check_block_file_metadata_import_update();

-- ============================================
CREATE OR REPLACE FUNCTION analysis.check_directory_entries_import_update() RETURNS trigger AS
$check$
BEGIN
    -- Check that the (supposedly immutable) data matches
    IF NEW.size IS DISTINCT FROM OLD.size THEN
        RAISE EXCEPTION 'size mismatch during import for analysis.block_id %: existing entry has %, new entry has %', OLD.block_id, OLD.size, NEW.size;
    END IF;

    RETURN NEW;
END;
$check$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER analysis_directory_entries_import_equality
    BEFORE UPDATE
    ON analysis.directory_entries
    FOR EACH ROW
    WHEN (OLD.size IS DISTINCT FROM NEW.size)
EXECUTE FUNCTION analysis.check_directory_entries_import_update();
