SELECT t.name AS unixfs_type, COUNT(*)
FROM block_links bl,
     block_stats bs,
     unixfs_types t
WHERE bl.block_id = bs.block_id
  AND bs.unixfs_type_id = t.id
GROUP BY t.name