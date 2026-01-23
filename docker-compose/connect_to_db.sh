#!/bin/bash

docker compose exec db psql -U ipfs_indexer -d ipfs_indexer
