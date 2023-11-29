SELECT 'f01' || to_hex(c.codec) || encode(b.multihash, 'hex') as cid, m.name
FROM cids c,
     blocks b,
     block_file_metadata f,
     mime_types m
WHERE c.block_id = b.id
  AND f.block_id = b.id
  AND m.id = f.libmagic_mime_type_id
  AND m.name = :mime_type
ORDER BY random()
LIMIT :num_rows;
