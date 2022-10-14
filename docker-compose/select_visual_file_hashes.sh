#!/bin/bash

set -e

# We need to remove the last line of psql output, which says something like "(n lines)"
psql -U ipfs_indexer -h 127.0.0.1 -p 5433 -f select_visual_file_hashes.sql -F ',' -A | head -n -1 | gzip -9 > visual_hashes.csv.gz
