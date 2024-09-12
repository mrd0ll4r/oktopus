SELECT block_size
FROM blocks b,
     cids c,
     block_stats bs
WHERE bs.block_id = b.id
  AND c.block_id = b.id
  AND c.codec = 112