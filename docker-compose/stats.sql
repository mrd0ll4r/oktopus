SELECT *
FROM (SELECT COUNT(*) as cid_cnt FROM blocks) AS cid_cnt,
     (SELECT COUNT(*) as block_cnt
      FROM block_stats) AS block_cnt,
     (SELECT COUNT(*) as file_cnt
      FROM block_file_hashes) AS file_cnt,
     (SELECT COUNT(DISTINCT block_id) as nonempty_dir_cnt
      FROM directory_entries) AS nonempty_dir_cnt,
     (SELECT COUNT(*) as successful_block_downloads
      FROM successful_downloads
      WHERE download_type_id = 1) AS successful_block_downloads,
     (SELECT COUNT(*) as successful_dag_downloads
      FROM successful_downloads
      WHERE download_type_id = 2) AS successful_dag_downloads,
     (SELECT COUNT(*) as failed_block_downloads
      FROM failed_downloads
      WHERE download_type_id = 1) AS failed_block_downloads,
     (SELECT COUNT(*) as failed_dag_downloads
      FROM failed_downloads
      WHERE download_type_id = 2) AS failed_dag_downloads,
     (SELECT COUNT(*) as direntry_cnt
      FROM directory_entries) AS direntry_cnt,
     (SELECT COUNT(*) as dag_link_cnt FROM block_links) AS dag_link_cnt
;