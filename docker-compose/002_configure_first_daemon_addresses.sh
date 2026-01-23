#!/bin/sh

set -ex

# Bump swarm addresses so the node correctly announces itself.
ipfs config --json 'Addresses.Swarm' '[
  "/ip4/0.0.0.0/tcp/4101",
  "/ip6/::/tcp/4101",
  "/ip4/0.0.0.0/udp/4101/quic",
  "/ip6/::/udp/4101/quic"
]'
