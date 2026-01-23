#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	CREATE USER ipfs_indexer;
	CREATE DATABASE ipfs_indexer;
	GRANT ALL PRIVILEGES ON DATABASE ipfs_indexer TO ipfs_indexer;
    ALTER USER ipfs_indexer WITH ENCRYPTED PASSWORD 'ipfs_indexer';
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "ipfs_indexer" <<-EOSQL
    CREATE USER ipfs_readonly;
    ALTER USER ipfs_readonly WITH ENCRYPTED PASSWORD 'ipfs_readonly';
    GRANT CONNECT ON DATABASE ipfs_indexer TO ipfs_readonly;
    GRANT USAGE ON SCHEMA public TO ipfs_readonly;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO ipfs_readonly;
    REVOKE CREATE ON SCHEMA public FROM PUBLIC;
    GRANT CREATE ON SCHEMA public TO ipfs_indexer;
EOSQL
