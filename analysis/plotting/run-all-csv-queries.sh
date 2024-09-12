#!/bin/sh
set -e

mkdir -p csv

rm csv/*.gz

for f in queries/*.sql; do
  echo "working on $f..."
  file_base=$(basename "$f" ".sql")

  for s in "public" "analysis"; do
    echo "querying in schema $s..."

    output_file=csv/${file_base}_${s}.csv

    ./run-csv-query.sh "$f" "$output_file" "$s"
    echo ""
  done
done
