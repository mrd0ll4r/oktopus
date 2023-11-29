#!/bin/bash

set -ex

user=ipfs
tmpdir="/ipfs-indexer/tmp"
downloads_dir="/ipfs-indexer/downloads"

if [ "$(id -u)" -eq 0 ]; then
  echo "Changing user to $user"
  # ensure temp folder is writable
  su-exec "$user" test -w "$tmpdir" || chown -R -- "$user" "$tmpdir"
  # ensure downloads folder is writable
  su-exec "$user" test -w "$downloads_dir" || chown -R -- "$user" "$downloads_dir"
  # restart script with new privileges
  exec su-exec "$user" "$0" "$@"
fi

# 2nd invocation with regular user
exec ./file-worker "$@"