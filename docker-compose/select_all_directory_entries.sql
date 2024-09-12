SELECT b2.id as parent_block_id,
'f01' || to_hex(c2.codec) || encode(b2.multihash, 'hex') as parent_cid,
b.id as block_id,
'f01' || to_hex(c.codec) || encode(b.multihash, 'hex') as cid,
d.name as name,
bm.file_size as file_size,
encode(bh.digest, 'hex') as sha256_hash
FROM directory_entries d
  INNER JOIN cids c ON d.referenced_cid_id = c.id
  INNER JOIN blocks b ON c.block_id = b.id
  LEFT OUTER JOIN block_file_metadata bm ON bm.block_id = b.id
  LEFT OUTER JOIN block_file_hashes bh ON bh.block_id = b.id,
  cids c2 INNER JOIN blocks b2 ON c2.block_id = b2.id
WHERE b2.id = d.block_id
AND bh.hash_type_id = 1 -- SHA256
;
