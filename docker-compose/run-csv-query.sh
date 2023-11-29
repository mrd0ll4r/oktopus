#!/bin/bash -e

pg_raw() {
  docker compose exec -T -u postgres db psql -d ipfs_indexer -v ON_ERROR_STOP=1 "$@"
}

if [ $# -eq 0 ]; then
  echo "No arguments supplied"
  exit 1
fi

query_file=$1
output_file=$2

if [ ! -f "$query_file" ]; then
  echo "missing file $query_file"
  exit 1
fi

echo "Reading query from $query_file..."
echo "Writing output to $output_file.gz ..."

query=$(cat "$query_file")

echo "Querying..."
pg_raw --csv -c "$query" | sed 's/\\x/f/g' | gzip -9 >"$output_file".gz
