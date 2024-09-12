SELECT 'f01' || to_hex(c.codec) || encode(b.multihash, 'hex') as cid, fm.file_size, foo.name AS direntry_name
FROM (SELECT DISTINCT de.name, c.id AS cid_id
FROM directory_entries de,
cids c, 
blocks b, 
block_file_metadata fm
WHERE de.referenced_cid_id = c.id 
AND c.block_id = b.id 
AND b.id = fm.block_id
AND fm.libmagic_mime_type_id = 8) foo,
cids c,
blocks b,
block_file_metadata fm
WHERE c.block_id = b.id
AND c.id = foo.cid_id
AND fm.block_id = b.id
