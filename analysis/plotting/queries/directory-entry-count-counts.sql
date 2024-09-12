SELECT foo.unixfs_type AS unixfs_type,
       foo.cnt         AS directory_entries,
       COUNT(*)        AS cnt
FROM (SELECT bs.block_id, t.name AS unixfs_type, COUNT(*) AS cnt
      FROM directory_entries de,
           block_stats bs,
           unixfs_types t
      WHERE bs.block_id = de.block_id
        AND bs.unixfs_type_id = t.id
      GROUP BY bs.block_id, t.name) foo
GROUP BY foo.unixfs_type, foo.cnt
