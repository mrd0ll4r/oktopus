#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	CREATE USER ipfs_indexer;
	CREATE DATABASE ipfs_indexer;
	GRANT ALL PRIVILEGES ON DATABASE ipfs_indexer TO ipfs_indexer;
        ALTER USER ipfs_indexer WITH ENCRYPTED PASSWORD 'ipfs_indexer';
EOSQL
