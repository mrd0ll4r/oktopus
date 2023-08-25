SELECT m.name, COUNT(*) as cnt
FROM block_file_metadata f,
     mime_types m
WHERE m.id = f.libmagic_mime_type_id
GROUP BY m.name
ORDER BY cnt DESC;
