ALTER TABLE successful_downloads RENAME COLUMN ts TO end_ts;
ALTER TABLE successful_downloads ADD start_ts TIMESTAMP WITH TIME ZONE;
ALTER TABLE successful_downloads ADD head_finished_ts TIMESTAMP WITH TIME ZONE;
ALTER TABLE successful_downloads ADD download_finished_ts TIMESTAMP WITH TIME ZONE;
