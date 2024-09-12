SELECT COUNT(*) AS parents
FROM block_links l,
     block_stats bs
WHERE l.block_id = bs.block_id
  AND (bs.unixfs_type_id = 2 OR bs.unixfs_type_id = 6)
GROUP BY l.referenced_cid_id
