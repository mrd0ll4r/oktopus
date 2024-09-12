#!/bin/bash -ex

docker compose exec -u postgres db psql -d ipfs_indexer
