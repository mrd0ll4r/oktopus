#!/bin/bash

set -ex

# Read INDEXER_FILE_STORAGE_UID and _GID from .env.
source .env

user_id=$INDEXER_FILE_STORAGE_UID
user_gid=$INDEXER_FILE_STORAGE_GID
if [ -z "$INDEXER_FILE_STORAGE_UID" ]; then
    echo "INDEXER_FILE_STORAGE_UID unset, using default value of 1000"
    user_id=1000
fi
if [ -z "$INDEXER_FILE_STORAGE_GID" ]; then
    echo "INDEXER_FILE_STORAGE_GID unset, using default value of 1000"
    user_gid=1000
fi
tmpdir="/ipfs-indexer/tmp"
downloads_dir="/ipfs-indexer/downloads"

if [ "$(id -u)" -eq 0 ]; then
  echo "Changing user to $user_id"
  # ensure temp folder is writable
  su-exec "$user_id" test -w "$tmpdir" || chown -R -- "$user_id:$user_gid" "$tmpdir"
  # ensure downloads folder is writable
  su-exec "$user_id" test -w "$downloads_dir" || chown -R -- "$user_id:$user_gid" "$downloads_dir"
  # restart script with new privileges
  exec su-exec "$user_id:$user_gid" "$0" "$@"
fi

# 2nd invocation with regular user
exec ./file-worker "$@"