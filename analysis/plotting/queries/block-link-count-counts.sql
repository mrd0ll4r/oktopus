SELECT r.name AS unixfs_type, r.num_links AS num_links, COUNT(*) AS cnt
FROM (SELECT bs.block_id, t.name, COUNT(*) AS num_links
      FROM block_links bl,
           block_stats bs,
           unixfs_types t
      WHERE bs.block_id = bl.block_id
        AND bs.unixfs_type_id = t.id
      GROUP BY bs.block_id, t.name) r
GROUP BY r.name, r.num_links
