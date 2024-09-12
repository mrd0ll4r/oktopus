#!/bin/bash -e

source ../.env

echo "Preparing diesel CLI..."
docker build -t diesel-cli -f Dockerfile.diesel .

diesel() {
  docker run --rm \
    -v "$(pwd)/../..":/app \
    --network="${COMPOSE_PROJECT_NAME}_default" \
    diesel-cli:latest \
    --database-url="$DATABASE_URL" \
    "$@"
}

pg_raw() {
  pushd ..
  docker compose exec -T -u postgres db psql -d ipfs_indexer -v ON_ERROR_STOP=1 "$@"
  popd
}

pg() {
  pg_raw -c "$1"
}

pg_script() {
  cat "$1" | pg_raw
}

echo "Setting up analysis database..."
# Make sure schema public is empty.
pg_script clear_schema_public.sql

# Set up schema public, move that over to analysis if it doesn't exist.
pg_script move_analysis_schema_to_public.sql
diesel setup
diesel migration run
pg_script move_public_schema_to_analysis_if_not_exists.sql

# Set up consistency triggers for schema analysis.
# These are idempotent, so no problem if we import them multiple times.
pg_script set_up_consistency_triggers.sql

echo "Starting data import (this will take a while)..."
dump_files=$(find ./db_dumps/ -name '*.sql.gz' | sort)
for f in $dump_files; do
  # Clear schema public again
  echo "Clearing import database..."
  pg_script clear_schema_public.sql

  echo "$f: Loading data..."
  zcat "$f" | pg_raw

  # Run all pending migrations on it
  echo "$f: Running migrations..."
  diesel migration run

  # Import it into the analysis database
  echo "$f: Importing into analysis database..."
  pg_script import.sql

  # Remove the file
  echo "$f: Import finished, marking file done."
  mv "$f" "$f.done"
done
