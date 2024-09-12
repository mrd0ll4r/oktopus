SELECT ut.name AS unixfs_type, COUNT(distinct block_stats.block_id) AS cnt
FROM block_stats
         INNER JOIN successful_downloads sd on block_stats.block_id = sd.block_id
         INNER JOIN unixfs_types ut on block_stats.unixfs_type_id = ut.id
WHERE sd.download_type_id = 2
GROUP BY ut.name;