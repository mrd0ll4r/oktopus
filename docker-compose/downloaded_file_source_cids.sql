WITH sources(
             source_cid_id,
             path,
             referenced_cid_id
    ) AS (
-- Calculate the transitive directory entries first
-- TODO it'd be better to do this the other way around, i.e., bottom-up.
-- That way, we could filter much earlier.
    WITH RECURSIVE r_dirents(
                             source_cid_id,
                             parent_cid_id,
                             referenced_cid_id,
                             path) AS
                       (SELECT c.id,
                               c.id,
                               d.referenced_cid_id,
                               ARRAY [c.id]
                        FROM cids c
                                 INNER JOIN blocks b ON (c.block_id = b.id)
                                 INNER JOIN directory_entries d ON (b.id = d.block_id)
                        UNION
                        SELECT rd.source_cid_id,
                               rd.referenced_cid_id,
                               d.referenced_cid_id,
                               rd.path || rd.referenced_cid_id
                        FROM r_dirents rd
                                 INNER JOIN cids c ON rd.referenced_cid_id = c.id
                                 INNER JOIN blocks b ON (c.block_id = b.id)
                                 INNER JOIN directory_entries d ON (b.id = d.block_id))
    SELECT rd.source_cid_id,
           rd.path,
           rd.referenced_cid_id

    FROM r_dirents rd,
         cids c1,
         blocks b1,
         cids c2,
         blocks b2
    WHERE rd.source_cid_id = c1.id
      AND c1.block_id = b1.id
      AND rd.referenced_cid_id = c2.id
      AND c2.block_id = b2.id
-- Add every file itself as its own source
    UNION
    SELECT c.id,
           ARRAY []::INTEGER[],
           c.id
    FROM cids c,
         blocks b,
         block_file_metadata bfm
    WHERE c.block_id = b.id
      AND bfm.block_id = b.id)
SELECT s.referenced_cid_id,
       JSON_AGG(JSON_BUILD_OBJECT(
               'path', s.path,
               'source_cid_id', s.source_cid_id,
               'source_cid', 'f01' || to_hex(c1.codec) || encode(b1.multihash, 'hex')
                )) as sources
FROM sources s,
     cids c1,
     blocks b1,
     -- Crude filter for now to only look at things we have downloaded.
     cids c2,
     block_file_metadata bfm
WHERE s.source_cid_id = c1.id
  AND c1.block_id = b1.id
  AND s.referenced_cid_id = c2.id
  AND c2.block_id = bfm.block_id
GROUP BY s.referenced_cid_id
;
