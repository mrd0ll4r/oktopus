SELECT m.name, COUNT(*)
FROM block_file_metadata f,
     mime_types m
WHERE m.id = f.mime_type_id
GROUP BY m.name
ORDER BY count DESC;
