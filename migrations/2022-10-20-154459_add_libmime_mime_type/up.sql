INSERT INTO mime_types(name)
VALUES ('unknown/pre-libmime-schema')
ON CONFLICT DO NOTHING;

ALTER TABLE block_file_metadata
    RENAME COLUMN mime_type_id TO freedesktop_mime_type_id;

ALTER TABLE block_file_metadata
    ADD libmime_mime_type_id INT REFERENCES mime_types (id);
UPDATE block_file_metadata
SET libmime_mime_type_id = (SELECT id
                            FROM mime_types
                            WHERE name = 'unknown/pre-libmime-schema');
ALTER TABLE block_file_metadata
    ALTER COLUMN libmime_mime_type_id SET NOT NULL;

