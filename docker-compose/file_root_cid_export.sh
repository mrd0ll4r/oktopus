#!/bin/bash -e

pg_raw() {
  docker compose exec -T -u postgres db psql -d ipfs_indexer -v ON_ERROR_STOP=1 "$@"
}

QUERY="$(cat downloaded_file_source_cids.sql)"

# Generate metadata snapshot
echo "Exporting roots..."
pg_raw --csv -c "$QUERY" |
    #Un-quote that cursed quoted JSON array
    mlr --icsv --ojson cat |
    jq -c '.[] | (.sources = [(.sources | fromjson | .[] | select (. != null) )])' |
    gzip -9 > roots.json.gz


