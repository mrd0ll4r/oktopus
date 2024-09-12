SELECT block_size, COUNT(*) as cnt
FROM blocks b,
     cids c,
     block_stats bs
WHERE bs.block_id = b.id
  AND c.block_id = b.id
  AND c.codec = 85
GROUP BY block_size