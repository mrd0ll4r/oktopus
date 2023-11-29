#!/bin/bash

docker compose exec -u postgres db psql -d ipfs_indexer
