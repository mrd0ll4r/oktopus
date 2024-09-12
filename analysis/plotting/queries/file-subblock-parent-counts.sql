SELECT COUNT(*) AS parents
FROM block_links l,
     block_stats bs
WHERE l.block_id = bs.block_id
  AND bs.unixfs_type_id = 3
GROUP BY l.referenced_cid_id
