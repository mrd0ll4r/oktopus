ALTER TABLE block_file_mime_types RENAME TO block_file_metadata;
ALTER TABLE block_file_metadata ADD file_size BIGINT;