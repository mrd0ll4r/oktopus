ALTER TABLE block_file_metadata
    DROP libmime_mime_type_id;
ALTER TABLE block_file_metadata
    RENAME COLUMN freedesktop_mime_type_id TO mime_type_id;

DELETE
FROM mime_types
WHERE name = 'unknown/pre-libmime-schema';
