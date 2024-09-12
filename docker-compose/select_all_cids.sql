SELECT b.id as block_id,
'f01' || to_hex(c.codec) || encode(b.multihash, 'hex') as cid
FROM cids c INNER JOIN blocks b ON c.block_id = b.id;
