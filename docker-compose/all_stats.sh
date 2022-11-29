#!/bin/bash -e

echo "It's now $(date)"
psql -U ipfs_indexer -h 127.0.0.1 -p 5433 -f storage_usage.sql -f stats.sql -f mime_type_counts.sql
