#!/bin/bash -e

mkdir -p db_dumps

docker compose exec -u postgres db pg_dump ipfs_indexer | gzip -9 > "db_dumps/$(date -u '+%Y-%m-%d_%H-%M_UTC').dump.sql.gz"
