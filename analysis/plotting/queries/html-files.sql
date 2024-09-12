SELECT 'f01' || to_hex(c.codec) || encode(b.multihash, 'hex') as cid, f.file_size
FROM cids c,
     blocks b,
     block_file_metadata f
WHERE c.block_id = b.id
  AND f.block_id = b.id
  AND f.libmagic_mime_type_id=8;
