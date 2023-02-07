#!/bin/bash

set -ex

user=ipfs
tmpdir="/ipfs-indexer/tmp"

if [ "$(id -u)" -eq 0 ]; then
  echo "Changing user to $user"
  # ensure folder is writable
  su-exec "$user" test -w "$tmpdir" || chown -R -- "$user" "$tmpdir"
  # restart script with new privileges
  exec su-exec "$user" "$0" "$@"
fi

# 2nd invocation with regular user
exec ./file-worker "$@"