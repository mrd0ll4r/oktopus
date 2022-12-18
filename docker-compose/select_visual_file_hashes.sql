SELECT 'f01' || to_hex(c.codec) || encode(b.multihash, 'hex') as cid,
       m.name                                                 as mime_type,
       encode(h.digest, 'hex')                                as sha2_256_hash
FROM cids c,
     blocks b,
     block_file_metadata f,
     block_file_hashes h,
     mime_types m
WHERE c.block_id = b.id
  AND f.block_id = b.id
  AND h.block_id = b.id
  AND h.hash_type_id = 1 -- SHA2_256
  AND m.id = f.libmime_mime_type_id
  AND (m.name ~ 'image/.*' OR m.name ~ 'video/.*');
