ALTER TABLE successful_downloads DROP start_ts;
ALTER TABLE successful_downloads DROP head_finished_ts;
ALTER TABLE successful_downloads DROP download_finished_ts;
ALTER TABLE successful_downloads RENAME COLUMN end_ts TO ts;
