SELECT f.file_size,
       m1.name AS libmagic_mime_type,
       m2.name AS freedesktop_mime_type
FROM block_file_metadata f,
     mime_types m1,
     mime_types m2
WHERE m1.id = f.libmagic_mime_type_id
  AND m2.id = f.freedesktop_mime_type_id
